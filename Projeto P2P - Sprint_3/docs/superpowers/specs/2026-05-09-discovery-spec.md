# Sprint 2.1 — Spec: Descoberta Dinâmica e Eleição de Master pelos Workers (2026-05-09)

**Fonte:** discovery.pdf — Prof. Michel Junio

**Resumo:** Workers iniciam sem IP/porta pré-configurados. Usam UDP Broadcast/Multicast para descobrir Masters disponíveis, elegem deterministicamente pelo MASTER_NAME (menor lexicográfico), conectam via TCP com handshake ELECTION_ACK e iniciam o Heartbeat da Sprint 1.

---

## Contexto do projeto

- Arquivos centrais: `masterp2.py` (porta TCP 7011) e `workerp2.py`
- Protocolo base: JSON + `\n` (Sprint 1)
- Esta sprint é PRÉ-REQUISITO para Sprint 1 — após ELECTION_ACK ACCEPTED, o fluxo continua normalmente com Heartbeat e tarefas

## Novos componentes (adição ao código existente)

### masterp2.py — adições

**Constantes:**
- `MASTER_NAME = 'MASTER_1'` — nome do master para eleição (configurável via `--master-name`)
- `DISCOVERY_PORT = 5000` — porta UDP para descoberta

**Função `discovery_listener()` (thread)**
- Escuta UDP na porta `DISCOVERY_PORT`
- Payload recebido: `{"TYPE": "DISCOVERY", "WORKER_UUID": "..."}`
- Resposta via UDP Unicast ao IP de origem do worker:
  ```json
  {"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_1", "MASTER_IP": "<IP real>", "MASTER_PORT": 7011, "STATUS": "AVAILABLE"}
  ```

**Handler TCP `handle_worker()` — novo caso `TYPE == ELECTION_ACK`:**
- Payload recebido: `{"TYPE": "ELECTION_ACK", "WORKER_UUID": "...", "SELECTED_MASTER": "MASTER_1"}`
- Resposta:
  ```json
  {"TYPE": "ELECTION_ACK", "STATUS": "ACCEPTED", "MASTER_NAME": "MASTER_1"}
  ```
- Após resposta: conexão encerrada (worker iniciará heartbeat)

### workerp2.py — adições

**Constantes:**
- `DISCOVERY_PORT = 5000`
- `DISCOVERY_MULTICAST = '239.255.255.250'`
- `DISCOVERY_WAIT = 3` (segundos para coletar respostas)
- `MASTER_IP = None` (default → ativa modo discovery)

**Método `_discover_masters(self) -> list`**
- Cria socket UDP com SO_BROADCAST
- Envia `{"TYPE": "DISCOVERY", "WORKER_UUID": self.uuid}\n` via broadcast (255.255.255.255) E multicast (239.255.255.250) na porta DISCOVERY_PORT
- Coleta respostas por `DISCOVERY_WAIT` segundos
- Filtra: só aceita payloads com TYPE=DISCOVERY_REPLY, MASTER_NAME, MASTER_IP, MASTER_PORT presentes
- Descarta payloads malformados com log warning (CT05)
- Deduplica por MASTER_NAME
- Retorna lista de dicts

**Método `_elect_master(self, masters: list) -> dict | None`**
- Elege pelo menor MASTER_NAME lexicográfico: `sorted(masters, key=lambda m: m['MASTER_NAME'])[0]`
- Log: `[ELECTION] Master eleito: MASTER_X`
- Retorna o dict do master eleito ou None se lista vazia

**Método `_connect_and_ack(self, master: dict) -> bool`**
- TCP connect ao master eleito (timeout 5s)
- Envia: `{"TYPE": "ELECTION_ACK", "WORKER_UUID": self.uuid, "SELECTED_MASTER": master_name}`
- Aguarda: `{"TYPE": "ELECTION_ACK", "STATUS": "ACCEPTED", "MASTER_NAME": "..."}`
- Se OK: chama `self.set_master(ip, port)`, retorna True
- Se falha de conexão ou resposta inesperada: log FALLBACK, retorna False

**Método `_discovery_loop(self) -> bool`**
- Loop com backoff exponencial (1s → 2s → 4s → ... → 60s)
- Etapas: _discover_masters → _elect_master → _connect_and_ack
- Se nenhum master: log `[FALLBACK] NO_MASTER_FOUND`, backoff, retry
- Se TCP falha: log FALLBACK, invalida eleição, retry (reset backoff)
- Retorna True quando conexão estabelecida com sucesso

**Modificação em `run(self)`**
- Se `self.master_ip is None` → chama `_discovery_loop()` antes de iniciar threads
- Se `--master-host` foi fornecido → skip discovery (compatibilidade)

**Modificação em `main()`**
- `--master-host` agora é opcional (não tem default `'masterp2'`)
- Sem `--master-host` → `MASTER_IP = None` → discovery ativo

## Regras de compatibilidade

- Não quebrar nenhum teste existente (tests/unit/, tests/integration/)
- Quando `--master-host` é fornecido: comportamento idêntico ao código atual
- Todos os payloads novos seguem JSON + `\n` com campos em CAIXA ALTA para controle
- Campos obrigatórios ausentes: ignorar payload silenciosamente, logar warning

## Critérios de Aceitação (DoD do discovery.pdf)

1. Worker inicia sem IP/porta configurados
2. Realiza descoberta e lista Masters respondentes
3. Elege consistentemente o MESMO master que outros Workers simultâneos
4. Estabelece TCP com o master eleito e envia primeiro Heartbeat com sucesso
5. Trata timeout (CT03), ausência de master (CT03), queda pós-eleição (CT04)
6. Payloads seguem JSON + `\n` com parsing estrito

## Estratégia de testes

- `tests/unit/test_election_by_name.py`: verifica eleição lexicográfica com múltiplos masters
- `tests/integration/run_discovery.py`: fake UDP master → worker descobre → elege → conecta TCP → verifica handshake

---

> Spec gerada em 2026-05-09. Plano em `docs/superpowers/plans/2026-05-09-discovery-plan.md`.
