# Migração FastStream/APScheduler → taskiq — considerações e sugestões

## O que foi entregue

Migração completa da camada de orquestração (`tasks/`) de **FastStream + RabbitBroker**
(mensageria) e **APScheduler** (cron) para **taskiq**, com:

- **Scheduler beat (cron)**: `tasks/scheduler.py`, usando `TaskiqScheduler` +
  `LabelScheduleSource`. Cada task periódica declara seu próprio cron via o label
  `schedule=[{"cron": "*/1 * * * *"}]` diretamente no decorator `@broker.task(...)`,
  substituindo o antigo padrão de "publicar um trigger" feito pelo APScheduler.
- **Scheduler com ETA**: `tasks/core/eta.py` (`schedule_task_at`), usando o mecanismo
  nativo de delay do `taskiq-aio-pika` (fila de delay com TTL + dead-letter de volta
  para a fila principal). Permite agendar qualquer task para rodar uma única vez em
  um instante futuro arbitrário — não apenas em intervalos fixos de cron.
- **Retry nativo (middleware)**: `SmartRetryMiddleware` no broker (`tasks/taskiq_app.py`),
  habilitado via `retry_on_error=True` em `process_msisdn_file`,
  `run_regular_campaign_execution` e nas três tasks de relatório — camada
  complementar ao retry de negócio já existente em `regular_campaign/retry.py`
  (ver item 6 das sugestões abaixo para a distinção entre os dois).
- **Broker único**: `tasks/taskiq_app.py`, um `AioPikaBroker` (RabbitMQ) roteando por
  `task_name` em vez das 6 `RabbitQueue`s específicas do FastStream.

### O que **não** foi alterado
- A lógica de negócio de todas as tasks já existentes (`msisdn/tasks.py`,
  `regular_campaign/tasks.py`, `regular_campaign/retry.py`, `reports/tasks.py`) foi
  preservada **linha a linha** — só o decorator/transporte mudou.
- `regular_campaign/workflow.py` foi deixado como **stub** (apenas a interface
  `RegularCampaignExecutionWorkflow.execute`, levantando `NotImplementedError`).
  A implementação real será feita separadamente; `regular_campaign/tasks.py` já a
  importa e chama exatamente como antes.
- `core/batching.py` e `regular_campaign/constants.py` — cópia exata, não dependiam
  de FastStream/APScheduler.
- `tasks/__init__.py` — mantido idêntico (os caminhos de import não mudaram).

### Validação feita
Todos os arquivos foram checados com `py_compile`. Além disso, montei stubs mínimos
de `geoloc_connector`/`geoloc_lib` e testei de verdade, com o `taskiq`/`taskiq-aio-pika`
instalados:
- `tasks.worker` importa e registra as 6 tasks corretamente, com o `SmartRetryMiddleware`
  anexado ao broker e os labels `retry_on_error`/`max_retries` corretos em cada task
  (e ausentes, de propósito, em `retry_failed_regular_campaign_dispatches`).
- `tasks.scheduler` + `LabelScheduleSource` lê os 4 crons declarados, sem interferência
  dos novos labels de retry.
- O import tardio em `retry.py` resolve o ciclo com `regular_campaign/tasks.py` sem erro.
- `retry_failed_dispatches()` executa toda a lógica original até o ponto de envio
  (`.kiq()`), falhando apenas por não haver um RabbitMQ real neste ambiente de teste
  — comportamento esperado.

## Como rodar

```bash
pip install taskiq taskiq-aio-pika

# Worker (consome as tasks)
taskiq worker tasks.worker:broker

# Scheduler / beat (dispara os crons)
taskiq scheduler tasks.scheduler:scheduler
```

`settings.CELERY_BROKER_URL` foi reaproveitado como URL do AMQP (mesmo valor usado
pelo `RabbitBroker` antigo) — nenhuma variável de ambiente nova é necessária para o
que foi entregue (o ETA usa a própria fila de delay do RabbitMQ, sem Redis).

---

## Sugestões de melhoria

1. **Dependências**: adicionar `taskiq` e `taskiq-aio-pika` ao `pyproject`/
   `requirements`, e remover `faststream` e `apscheduler` quando a migração for
   totalmente adotada em produção.

2. **Isolamento de filas por domínio**: hoje há **uma única fila** RabbitMQ para todas
   as tasks (roteamento por `task_name`), enquanto o FastStream tinha 6 filas
   dedicadas (`msisdn.process`, `campaign.exec`, `campaign.retry`, `reports.*`). Isso
   simplifica a configuração, mas perde a possibilidade de escalar/priorizar workers
   por domínio de forma independente (ex.: mais workers para `msisdn` sem afetar
   `reports`). Se isso for importante, dá para instanciar múltiplos `AioPikaBroker`
   (um por domínio, cada um com seu `task_queues`) e rodar processos `taskiq worker`
   separados por broker.

3. **Retry sem backoff real**: o `retry.py` original já republicava a execução
   *imediatamente* (`next_retry_at=reference`, ou seja, "agora") — mantive esse
   comportamento exatamente. Vale avaliar usar `schedule_task_at` (o novo helper de
   ETA) para introduzir um backoff real (ex.: crescente por `dispatch_attempts`),
   evitando que falhas recorrentes gerem rajadas de retry no mesmo minuto.

4. **ETA integrado ao workflow**: quando `regular_campaign/workflow.py` for
   implementado e a execução for pausada por `OUTSIDE_ALLOWED_WINDOW`, em vez de só
   aguardar o próximo poll do cron (`retry_failed_dispatches`, a cada minuto), o
   workflow pode chamar `schedule_regular_campaign_execution(..., run_at=<início da
   próxima janela permitida>)` para retomar a execução no instante exato — reduzindo
   latência e a quantidade de polls desnecessários.

5. **Result backend**: nenhum result backend foi configurado (igual ao FastStream
   original, que também não expunha resultados). Se monitoramento/observabilidade dos
   resultados das tasks for necessário, considerar `taskiq-redis`
   (`RedisAsyncResultBackend`) ou similar.

6. **Middleware de retry nativo (implementado)**: adicionado `SmartRetryMiddleware`
   ao broker (`tasks/taskiq_app.py`), com backoff exponencial + jitter, usando a
   mesma fila de delay já configurada para o ETA (sem depender de Redis). Habilitado
   via `retry_on_error=True` em `process_msisdn_file`, `run_regular_campaign_execution`
   e nas três tasks de `reports/tasks.py` — todas já deixavam a exceção escapar após
   logar (`except Exception: ... raise`), então esse era o padrão natural para se
   beneficiar do retry automático.

   Importante: isso é **complementar**, não substitui `regular_campaign/retry.py`.
   O middleware só reage a uma exceção levantada *durante uma execução ao vivo* de
   uma task; `retry.py` é uma reconciliação periódica sobre estado **persistido no
   banco** (`dispatch_attempts`, `next_retry_at`, corte por idade desde
   `created_at`), cobrindo cenários que o middleware não alcança — mensagem nunca
   entregue, worker que morre sem lançar exceção, ou falhas de negócio que o
   `workflow` já trata internamente (atualizando status sem levantar exceção). Por
   isso `retry_failed_regular_campaign_dispatches` foi deixada de fora do
   `retry_on_error` — ela já roda a cada minuto via cron, e um retry adicional no
   nível de mensagem não agregaria valor.

7. **Testes sem RabbitMQ**: o taskiq oferece um `InMemoryBroker`, útil para testes de
   unidade das tasks sem precisar subir RabbitMQ real. Recomendo criar um `conftest.py`
   que troca o broker por essa implementação em ambiente de teste.

8. **Nomes de task estáveis**: os `task_name` explícitos (`msisdn.process_msisdn_file`,
   `regular_campaign.run_execution` etc.) foram escolhidos deliberadamente. Evitar
   renomeá-los em deploys futuros sem cuidado, pois mensagens já publicadas com o
   nome antigo (inclusive na fila de delay) deixariam de encontrar handler.

9. **Alta disponibilidade do scheduler**: a documentação do taskiq recomenda rodar
   **apenas uma instância** do processo `taskiq scheduler` por ambiente — múltiplas
   réplicas disparariam os mesmos crons em duplicidade (não há leader election
   embutida). Vale configurar isso explicitamente na infraestrutura (réplica única
   ou eleição de líder externa).

10. **Cadência dos crons**: os quatro jobs recriados (`retry_failed_dispatches`,
    `generate_hourly_report`, `generate_daily_report`, `generate_message_report`)
    usam `"*/1 * * * *"` (a cada minuto) porque foi exatamente o que constava no
    `APScheduler` original reconstruído a partir das capturas de tela. Vale
    confirmar se esse intervalo é intencional para os relatórios "diário"/"horário"
    antes de considerar a migração encerrada.

11. **Configuração de settings**: nenhuma configuração nova precisou ser adicionada.
    Se no futuro for adotado o plugin `x-delayed-message` do RabbitMQ (em vez da
    fila de delay simples usada aqui) para ETA de longuíssimo prazo, será necessário
    habilitar o plugin no broker e ajustar `delayed_message_exchange_plugin=True`
    em `tasks/taskiq_app.py`.
