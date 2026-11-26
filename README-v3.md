# GeoForest Lab — V3 experimental

Interface web independente de Streamlit, servida por FastAPI com MapLibre GL e Plotly.js (CDNs, conexão internet necessária). Sem Node.js nesta etapa; React, tiles vetoriais e novas bases ambientais ainda não implementados.

## Debian — reaproveitar instalação V2 funcional

Na pasta existente desmatamento-v2:

```bash
git fetch origin develop/v3
git switch -c develop/v3 --track origin/develop/v3
source .venv/bin/activate
python -m pip install -r requirements-v3.txt
python -m pytest -q
python -u research_grid.py --cell-km 5
python -m uvicorn api_v3:app --host 127.0.0.1 --port 8000
```

Se a configuração single-branch impedir o switch acima, use git fetch origin develop/v3:refs/remotes/origin/develop/v3 e git switch -c develop/v3 refs/remotes/origin/develop/v3. Não apague data/. Abra http://127.0.0.1:8000. Swagger: /docs.

Em instalação nova, clone develop/v3 e execute python -u pipeline_v2.py antes de research_grid.py. A V3 usa os resultados V2 e não baixa novamente os dados quando já existem. Para análises de sensibilidade, execute research_grid.py --cell-km 10 ou 20 (2 km também disponível, maior custo). Gere as grades antes de iniciar o servidor.

## Métodos e interpretação

- Grade quadrada ancorada na origem da projeção LAEA regional, recortada ao estado; área territorial exata na projeção. Unidades de borda têm área menor.
- Distância do centroide da célula à estrada mais próxima em AEQD regional; densidade viária em km/km² (comprimento planar LAEA). Não equivale à área exata dos buffers da V2.
- Proporção de incremento PRODES / área territorial: não mede risco condicionado à floresta remanescente. Células sem incremento são incluídas. Anos ausentes não são imputados.
- Spearman descritivo, sem p-valor convencional devido à autocorrelação.
- Moran global com pesos rook por índices da grade, padronizados por linha, ilhas removidas, permutação global bilateral em torno de -1/(n-1). LISA: I_i = z_i sum(w_ij z_j), permutação condicional bilateral aproximada centrada na média simulada, correção BH a 5%. Seed 42. Validação cruzada com PySAL e análise de sensibilidade necessárias antes de publicação. Vizinhos definidos por grade, não por contiguidade física exata de fragmentos recortados. FDR depende do número de permutações; poucas permutações reduzem poder.
- OSM contemporâneo: sem precedência temporal; extrato brasileiro sem estradas internacionais. PRODES florestal não cobre toda supressão de lavrado. Não fazer inferência causal.
- CSV por célula e ano; gráficos exportáveis em SVG pelo menu Plotly. Manifesto de origem da V2.

## Próximas etapas

MapBiomas, vegetação disponível por ano, áreas protegidas, relevo, categorias viárias e históricos; modelos multivariados com diagnóstico espacial e validação espacial; polígonos personalizados; comparação entre grades; tiles vetoriais; frontend modular React. Não há regressão espacial implementada nesta etapa.

Testes sintéticos incluídos, ainda sem validação integral com dados reais desta V3.
