# Sprint 3 — Brainstorm: Protocolo M2M e Redirecionamento Dinâmico de Workers (2026-05-17)

**Contexto:** Implementar a camada de comunicação P2P entre Masters para que um Master saturado possa negociar e receber Workers emprestados de um Master vizinho. Baseia-se nos arquivos `masterp2.py` e `workerp2.py` (Sprint 2.1 já funcional).

---

## 1) Questões abertas levantadas antes de implementar

### Q1: Como o Master B envia `command_redirect` ao Worker B1?

O spec diz "pela conexão de socket que ele já mantém com eles". Mas no protocolo atual (Sprint 2), cada Worker abre **uma nova conexão TCP por ciclo de tarefa** — não há conexão persistente.

**Opções analisadas:**

| Opção | Descrição | Pros | Contras |
|-------|-----------|------|---------|
| A — Piggyback no próximo ALIVE | Quando Worker conecta com ALIVE, Master verifica se há redirect pendente e responde `command_redirect` em vez de QUERY/NO_TASK | Zero mudança na infraestrutura de conexão; simples | Depende do Worker reconectar; redirect não é imediato |
| B — Master B abre conexão reversa ao Worker | Worker expõe porta extra (servidor) | Imediato | Requer Worker como servidor; colisão com PEER_PORT 5001 |
| C — Worker mantém conexão persistente | Refatora para keep-alive TCP | Mais robusto | Quebra toda a Sprint 2; escopo enorme |

**Decisão: Opção A.** Workers reconectam a cada `TASK_INTERVAL=5s` — a latência do redirect é aceitável para o demo. Não quebra nada existente.

---

### Q2: Como `command_release` chega ao Worker emprestado?

Mesmo problema: o Worker emprestado está conectado ao Master A por ciclos curtos.

**Decisão:** Mesma solução — piggybacking no ALIVE. Quando Worker B1 envia ALIVE ao Master A e este detecta que o worker está em `release_pending`, responde com `command_release` em vez de QUERY/NO_TASK.

---

### Q3: Como o Master B sabe quais Workers oferecer em `response_accepted`?

Não há lista persistente de Workers no Master atual. Workers só aparecem quando conectam.

**Opções:**
- **A — `known_workers` dict:** Rastrear cada ALIVE recebido em um dicionário `{uuid → last_seen}`. Ao receber `request_help`, selecionar Workers vistos nas últimas 60s que não estejam cedidos.
- **B — Contagem sem UUID:** Manter apenas um contador de "próximos N redirects" sem fixar UUIDs. Qualquer Worker que conectar será redirecionado.

**Decisão: Opção A.** Permite incluir `worker_details` com IDs reais na `response_accepted`, satisfazendo o campo exigido pelo spec. Opção B é mais robusta em ambiente de falhas, mas perde rastreabilidade.

---

### Q4: Como distinguir mensagem M2M de mensagem Worker na mesma porta TCP?

O spec exige que o mesmo socket (porta 7011) aceite conexões de Workers AND de outros Masters.

**Análise:** Mensagens de Workers usam `"TASK"`, `"WORKER"`, `"TYPE"` (maiúsculos). Mensagens M2M usam `"type"` (minúsculo) conforme spec. O campo `"type"` minúsculo é exclusivo do protocolo M2M Sprint 3.

**Decisão:** Em `handle_worker()`, verificar `"type" in message and "TYPE" not in message`. Se verdadeiro, despachar para `handle_m2m()`. Caso contrário, fluxo normal Sprint 1/2.

---

### Q5: Qual porta usar para o `_my_address` passado em `request_help`?

O Master A precisa informar ao Master B o seu endereço para que os Workers redirecionados saibam onde se conectar.

**Decisão:** Calcular `get_my_ip() + ":" + str(PORT)` em `start_master()` e armazenar em `_my_address` global. Passar em `payload.master_address` do `request_help`. O spec não proíbe campos extras no payload.

---

### Q6: Como evitar negociações M2M concorrentes (dois threads de saturação disparando ao mesmo tempo)?

**Decisão:** Usar `threading.Lock()` não-bloqueante (`_negotiating.acquire(blocking=False)`). Se não conseguir adquirir, o ciclo de saturação desse tick é ignorado.

---

## 2) Riscos identificados

| Risco | Probabilidade | Impacto | Mitigação |
|-------|--------------|---------|-----------|
| Worker redirecionado não reconecta (crash) | Baixa | Médio | Redirect fica em `redirect_targets` indefinidamente; aceitável no demo |
| Saturação e liberação em ping-pong | Média | Alto | Histerese: `RELEASE_THRESHOLD < CAPACITY` (60%) evita oscilação |
| Deadlock em locks aninhados | Baixa | Alto | Nunca adquirir dois locks ao mesmo tempo; usar cópias locais antes de operar |
| `known_workers` cresce sem limite | Baixa | Baixo | Workers inativos naturalmente ficam com `last_seen` antigo e são ignorados |
| `notify_worker_returned` falha se Master B reiniciou | Média | Baixo | Apenas log; não afeta operação de Master A |

---

## 3) Estrutura de mensagens M2M decidida

Todas as mensagens usam delimitador `\n` e estrutura:
```json
{
  "type": "TIPO",
  "request_id": "uuid-v4",
  "payload": { }
}
```

| `type` | Direção | Campos obrigatórios no payload |
|--------|---------|-------------------------------|
| `request_help` | A → B | `master_id`, `master_address`, `current_load`, `capacity`, `workers_needed` |
| `response_accepted` | B → A | `workers_offered`, `worker_details` |
| `response_rejected` | B → A | `reason` |
| `command_redirect` | B → Worker | `new_master_address` |
| `register_temporary_worker` | Worker → A | `worker_id`, `original_master_address` |
| `command_release` | A → Worker | `original_master_address` |
| `notify_worker_returned` | A → B | `worker_id` |

**Nota de compatibilidade:** `request_id` da resposta (`response_accepted`/`response_rejected`) é idêntico ao da requisição original para correlação em ambientes com múltiplos pedidos simultâneos.

---

## 4) Arquitetura de estado no Master

```
known_workers         {uuid → {last_seen, addr}}
  ↓ (quando request_help aceito)
redirect_targets      {uuid → {new_master_address}}
  ↓ (quando Worker conecta e há redirect pendente)
lent_workers          {uuid}
  ↓ (quando register_temporary_worker chega)
borrowed_workers      {uuid → {original_master_address, since}}
  ↓ (quando load < RELEASE_THRESHOLD)
release_pending       {uuid → {original_master_address}}
  ↓ (quando Worker conecta e há release pendente)
→ notify_worker_returned → Master B remove de lent_workers
```

---

## 5) Arquitetura de estado no Worker

```
Estado inicial: borrowed_from = None, master_ip = Master B

→ recebe command_redirect:
    borrowed_from = "Master_B:port"
    master_ip = Master A
    envia register_temporary_worker para Master A
    próximos ALIVE: SERVER_UUID = borrowed_from

→ recebe command_release:
    borrowed_from = None
    master_ip = Master B (original)
    próximos ALIVE: sem SERVER_UUID
```

---

## 6) O que NÃO foi implementado (fora do escopo desta sprint)

- Pool de conexões M2M persistente (recomendado pelo spec como otimização) — conexões são abertas e fechadas por negociação
- Timeout de stale entries em `redirect_targets` — se Worker nunca voltar, o redirect fica pendente
- Múltiplos neighbors simultâneos em paralelo — a saturação tenta vizinhos em sequência
- Interface gráfica / dashboard de monitoramento

---

> Gerado em 2026-05-17. Spec em `docs/superpowers/specs/2026-05-17-sprint-3-spec.md`. Plano em `docs/superpowers/plans/2026-05-17-sprint-3-plan.md`.
