# Sprint 2.1 — Discovery Brainstorm (2026-05-09)

**Contexto (discovery.pdf):** Workers devem iniciar SEM nenhum IP/porta de Master pré-configurado. Realizam descoberta via UDP Multicast/Broadcast, elegem o Master pelo nome (ordem lexicográfica crescente), conectam via TCP, fazem handshake (ELECTION_ACK) e iniciam o ciclo de Heartbeat da Sprint 1.

**Diagnóstico do código existente:**
- O código feito com Gemini implementou a sprint ERRADA — a eleição existente em `workerp2.py` é worker-to-worker (para eleger master temporário quando o master original cai). Isso é Sprint 1/2.2.
- A Sprint 2.1 trata de DESCOBERTA INICIAL: "como o worker acha o master" antes de qualquer heartbeat.
- `masterp2.py` não escuta UDP, não tem `MASTER_NAME`, não trata `ELECTION_ACK`.
- `workerp2.py` inicia com `MASTER_IP` hardcoded — viola o requisito O1.
- Tudo que foi feito anteriormente (REQUEUE fix, testes) está correto e deve ser MANTIDO. Apenas adicionar o mecanismo de descoberta em cima.

---

## 1) Requisitos do discovery.pdf

**O1.** Worker inicia sem IP/porta configurados.
**O2.** Eleição baseada EXCLUSIVAMENTE em `MASTER_NAME` (lexicográfico: MASTER_1 < MASTER_10 < MASTER_2 via `sorted()`).
**O3.** Todos os Workers convergem para o MESMO Master sem comunicação entre si.
**O4.** Transição segura UDP → TCP com handshake ELECTION_ACK.
**O5.** Resiliência: backoff, retry, reinício de descoberta se conexão TCP falhar.

---

## 2) Payloads do protocolo (conforme discovery.pdf)

```json
// Worker → UDP Broadcast/Multicast
{"TYPE": "DISCOVERY", "WORKER_UUID": "W-101"}

// Master → UDP Unicast ao worker
{"TYPE": "DISCOVERY_REPLY", "MASTER_NAME": "MASTER_1", "MASTER_IP": "192.168.1.20", "MASTER_PORT": 7011, "STATUS": "AVAILABLE"}

// Worker → Master TCP
{"TYPE": "ELECTION_ACK", "WORKER_UUID": "W-101", "SELECTED_MASTER": "MASTER_1"}

// Master → Worker TCP
{"TYPE": "ELECTION_ACK", "STATUS": "ACCEPTED", "MASTER_NAME": "MASTER_1"}
```

---

## 3) Abordagens propostas

**A. Additive — adicionar discovery em cima do código existente (RECOMENDADA)**
- `MASTER_IP = None` como default → ativa modo discovery
- Se `--master-host` fornecido → skip discovery (compatibilidade com testes existentes)
- Adicionar métodos `_discover_masters()`, `_elect_master()`, `_connect_and_ack()`, `_discovery_loop()` no Worker
- Adicionar UDP listener + ELECTION_ACK handler no master
- Não mexer nos loops de heartbeat, eleição worker-to-worker, temp master
- Benefício: zero risco de regressão; código de Sprint 1 preservado
- Custo: `workerp2.py` fica maior (aceitável para projeto acadêmico)

**B. Refatoração com módulo p2p/discovery.py**
- Extrair descoberta para módulo separado
- Benefício: mais limpo
- Risco: escopo maior, mais tempo, possível regressão

**C. Substituição completa do startup**
- Remover MASTER_IP hardcoded, forçar discovery em todos os casos
- Risco: quebra testes e runners que passam --master-host

**Recomendação:** A — additive. Mantém tudo que funciona e adiciona o que falta.

---

## 4) Fluxo de descoberta

```
Worker inicia
    |
    v
MASTER_IP configurado? ——Yes——> usar IP direto (compatibilidade)
    |
    No
    v
Enviar UDP BROADCAST/MULTICAST
{"TYPE": "DISCOVERY", "WORKER_UUID": "..."}
    |
    v
Aguardar 3s coleta respostas DISCOVERY_REPLY
    |
    No masters? ——> log NO_MASTER_FOUND ——> backoff exp. ——> retry
    |
    Masters encontrados
    v
Eleger: sorted(masters, key=MASTER_NAME)[0]
    |
    v
TCP connect ao master eleito (timeout 5s)
    |
    Falha? ——> invalidar cache ——> reiniciar discovery
    |
    Sucesso
    v
Enviar ELECTION_ACK TCP ——> receber ACCEPTED
    |
    v
Iniciar loop Heartbeat + Tarefas (Sprint 1)
```

---

## 5) Cenários de Teste (CT do discovery.pdf)

| CT | Cenário | Ação Worker | Critério |
|----|---------|-------------|---------|
| CT01 | 1 Master | DISCOVERY → 1 reply | Conecta ao MASTER_1, inicia HB |
| CT02 | 3 Masters | Recebe MASTER_1, MASTER_2, MASTER_3 | Elege MASTER_1 (lexicogr.) |
| CT03 | Nenhum Master | Timeout 3s | Loga NO_MASTER_FOUND, backoff, retry |
| CT04 | Master cai após TCP | ConnectionError | Invalida cache, reinicia discovery |
| CT05 | Payload malformado | JSON inválido/sem MASTER_PORT | Descarta, loga warning, continua |

---

> Brainstorm gerado em 2026-05-09. Spec em `docs/superpowers/specs/2026-05-09-discovery-spec.md`.
