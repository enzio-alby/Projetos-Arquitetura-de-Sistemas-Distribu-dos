# Projeto P2P — Brainstorm (2026-05-04)

**Contexto resumido:** Repositório com `master.py` (server/master) e `worker/worker.py` (worker P2P). Objetivo: implementar três tarefas (01, 02, 03) sem causar regressões.

**Meta do brainstorm:** coletar opções de abordagem antes de tomar decisões estruturais, identificar riscos e recomendar a estratégia incremental que minimize regressões.

---

## 1) Restrições e requisitos
- Não introduzir regressões nem alterações disruptivas na API existente (protocolos TCP/JSON entre master e workers).
- Testes automatizados obrigatórios para cada mudança.
- Mudanças pequenas, com commits frequentes e reversão fácil.

## 2) Perguntas de clarificação (sugeridas)
1. Qual é a prioridade entre as três tarefas (urgência)?
2. Há um ambiente CI já configurado (GitHub Actions, GitLab CI)?
3. Podemos adicionar dependências (ex.: pytest, requests) se necessário?

(Perfeito para perguntar um por vez — seguir a regra do `brainstorming`.)

## 3) Abordagens propostas (2–3) com trade-offs

A. Abordagem incremental conservadora (recomendada)
- Fazer alterações pequenas e testáveis em `worker/worker.py` e `master.py`.
- Cobrir comportamento com testes unitários e um teste de integração leve.
- Benefício: mínimo risco de regressão; fácil rollback.
- Custo: progresso mais lento, mais commits.

B. Abordagem modularizadora
- Extrair responsabilidades em novos módulos (ex.: `p2p/heartbeat.py`, `p2p/election.py`) e adaptar o código para usar as novas APIs.
- Benefício: melhor manutenção e testes unitários mais isolados.
- Risco: alterações maiores que podem provocar regressões; exige mais cobertura de testes.

C. Abordagem de feature-flag
- Implementar mudanças atrás de flags (variáveis de ambiente ou configurações) ativáveis para testes.
- Benefício: permite deploy seguro e rollback rápido.
- Custo: complexidade adicional no runtime e test matrix.

Recomendação: começar pela A (incremental), com preparação para migrar para B se a complexidade crescer; usar C apenas se o deploy for ao vivo em múltiplos hosts.

## 4) Riscos e mitigação
- Risco: testes ausentes → regressão: Mitigar escrevendo testes primeiro (TDD).
- Risco: bloqueio por dependência de runtime: Mitigar com mocks e testes de integração isolados.

## 5) Próximo passo imediato
- Fazer uma pergunta de clarificação: "Deseja priorizar alguma das tarefas 01/02/03 (ex.: ordem de entrega)?" (Enviar só uma pergunta por mensagem)

---

> Arquivo gerado automaticamente pelo workflow de skills. Após sua resposta, escreverei o design (spec) e seguirei com o `writing-plans` para gerar o plano detalhado.
