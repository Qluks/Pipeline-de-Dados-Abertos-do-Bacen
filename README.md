# Bacen Data Pipeline — Etapa 1: Ingestão

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m src.ingestion.run_ingestion
```

Isso vai ler `config/series.yaml`, buscar cada série na API do SGS/Bacen
e salvar o payload bruto em:

```
data/bronze/serie={codigo}/data_ingestao={AAAA-MM-DD}/raw.json
```

## Adicionar uma nova série

Basta acrescentar um item em `config/series.yaml` — não precisa mexer
em nenhum código:

```yaml
  - codigo: 433
    nome: ipca_mensal
    data_inicial: "01/01/2015"
```

## O que acontece por baixo dos panos

- `fetch_serie()` quebra o intervalo total em janelas de 1 ano e faz
  uma requisição por janela, com retry exponencial (`tenacity`) em
  caso de falha de rede ou resposta inesperada da API.
- Uma série que falhar **não** derruba as outras — o erro é logado e
  a ingestão segue para a próxima série da lista.
- `save_raw()` grava o JSON bruto (sem nenhuma transformação) mais um
  pequeno bloco de metadata (`codigo`, `nome`, `data_ingestao`,
  `quantidade_registros`).

## Próximos passos (fora do escopo desta etapa)

- Transformação bronze → silver → gold com PySpark + Delta Lake
- Testes automatizados com pytest
- Validação de qualidade com Great Expectations

## Etapa 2: Transformação (bronze -> silver -> gold)

```bash
python -m src.transform.run_transform
```

Isso lê tudo em `data/bronze/`, grava:
- `data/silver/series` — dados limpos, tipados, deduplicados (Delta)
- `data/gold/mensal_por_serie` — agregação mensal + variação % (Delta)
- `data/gold/comparativo_mensal` — séries lado a lado, por mês (Delta)

**Nota:** na primeira execução, o Delta Lake baixa algumas dependências
do Maven Central automaticamente — isso é esperado e só acontece uma vez
(fica em cache depois).

### Conferir o resultado (Python/PySpark)

```python
from src.transform.spark_session import get_spark_session

spark = get_spark_session()
spark.read.format("delta").load("data/gold/comparativo_mensal").show()
```

## Etapa 3: Testes automatizados

```bash
pytest -v
```

18 testes cobrindo:
- **Ingestão** (`test_ingestion.py`): chunking de janelas anuais, agregação de múltiplas páginas, tratamento de erro HTTP/formato inválido, gravação em bronze — tudo com a API mockada (nenhuma chamada de rede real).
- **Transformação** (`test_transform.py`): conversão de vírgula decimal, dedup mantendo a ingestão mais recente, remoção de nulos, agregação mensal, cálculo de variação percentual, pivot do comparativo — usando uma `SparkSession` local de teste.

O arquivo `pytest.ini` já configura o `pythonpath`, então basta rodar `pytest` na raiz do projeto.

## Etapa 4: Qualidade de dados (Great Expectations)

A validação roda automaticamente como parte de `python -m src.transform.run_transform`,
entre a etapa silver e a gold — se falhar, o pipeline para e a gold **não** é gerada.

Checagens aplicadas na camada silver:
- `data` e `valor` nunca nulos
- Sem duplicata de (série, data)
- `valor` dentro de um range **específico por série** (ex: SELIC entre 0-100,
  IPCA mensal entre -5 e 5) — configurável em `src/quality/expectations.py`,
  dicionário `RANGES_POR_SERIE`

Pra rodar sem a validação (debug pontual, não recomendado):
```bash
python -m src.transform.run_transform --skip-quality-check
```

Testes: `pytest tests/test_quality.py -v`

## Etapa 5: Dashboard (Streamlit)

```bash
streamlit run src/dashboard/app.py
```

Decisão técnica importante: o dashboard lê as tabelas gold com
**`deltalake` (delta-rs)**, não com PySpark. O pipeline de transformação
precisa do Spark pra processar/agregar os dados, mas o dashboard só
precisa *ler* duas tabelas pequenas já prontas — subir uma JVM inteira
via Spark só pra isso adicionaria segundos de latência a cada acesso,
sem necessidade. `deltalake` lê o mesmo formato aberto do Delta Lake,
sem precisar de Java.

O app tem:
- Filtro de séries e intervalo de datas (sidebar)
- KPIs com o valor mais recente + variação % de cada série selecionada
- Gráfico de série temporal (uma linha por indicador)
- Comparativo entre indicadores no mesmo período
- Tabela detalhada (expansível)

Pra publicar de graça: suba o repositório no GitHub e conecte em
https://share.streamlit.io (Streamlit Community Cloud) — ele builda
direto do `requirements.txt` e do `src/dashboard/app.py`.