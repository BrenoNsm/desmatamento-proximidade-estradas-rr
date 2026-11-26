# V2 experimental — pesquisa geoespacial

A versão original permanece em main. A V2 é independente e usa data/v2. Python 3.11 ou 3.12 recomendado.

## Windows PowerShell

```powershell
git clone --branch develop/v2 --single-branch https://github.com/BrenoNsm/desmatamento-proximidade-estradas-rr.git desmatamento-v2
cd desmatamento-v2
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-v2.txt
Copy-Item research_v2.example.json research_v2.json
```

Copie as pastas de dados brutos do projeto original (data/external, data/osm, data/prodes) para data/ nesta cópia, preservando os arquivos auxiliares dos shapefiles. Ajuste os caminhos no JSON conforme necessário.

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe pipeline_v2.py --config research_v2.json
.\.venv\Scripts\python.exe -m streamlit run app_v2.py
```

Abra http://localhost:8501. Para atualizar o código: git pull origin develop/v2.

## Fontes remotas

Cada fonte no JSON pode ser um caminho local ou um objeto como:
```json
{"url":"https://servidor-oficial/arquivo.zip","member":"pasta/arquivo.shp","sha256":"HASH_REAL_OPCIONAL"}
```
Substitua pelos URLs reais dos arquivos ZIP e pelos nomes internos corretos; o exemplo acima não é um endpoint válido. O pipeline baixa, verifica o hash opcional, extrai e registra proveniência. Não há descoberta automática de novas publicações ou agendamento. Consulte os portais indicados no README original. Os arquivos locais também têm hashes registrados. Para cobertura internacional, forneça uma malha viária consolidada com as estradas vizinhas.

## Método

- Limite estadual obtido pela união dos 15 municípios IBGE (código 14).
- Projeção azimutal equidistante regional para buffers aproximados; projeção azimutal equivalente para áreas.
- Estradas recortadas em halo de 20 km; anéis recortados posteriormente ao estado.
- Interseções efetivas por município, ano e faixa; verificação de conservação de área.
- Agregados DuckDB, GeoParquet e manifesto de fontes.
- Interface Streamlit/PyDeck WebGL; leitura seletiva e camada de desmatamento opcional. Ainda não utiliza tiles vetoriais.
- Indicador de proporção territorial, não taxa anual nem medida causal. O denominador inclui áreas não florestais. A malha viária contemporânea não comprova precedência temporal.

## Limitações e validação

Protótipo: testes sintéticos fornecidos, ainda sem execução confirmada com dados reais. Compare totais com a versão original, valide cobertura de estradas e PRODES, precisão dos buffers e ausência de sobreposições entre polígonos de origem. Não execute processamento simultaneamente com a interface: a publicação dos arquivos não é transacional. Relatórios PDF, áreas personalizadas, tiles e frontend React permanecem fora desta etapa. Não use os resultados para publicação antes da validação metodológica.
