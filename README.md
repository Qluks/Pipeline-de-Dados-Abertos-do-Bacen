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