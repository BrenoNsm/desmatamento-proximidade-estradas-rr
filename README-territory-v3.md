# Contexto territorial INPE — V3

Com a grade existente:

```bash
git pull origin develop/v3
source .venv/bin/activate
python -m pytest -q
python -u territory_v3.py --cell-km 5
python -m uvicorn api_v3:app --host 127.0.0.1 --port 8000
```

O módulo consulta o WFS público que já funciona na V2/V3, com os nomes confirmados no catálogo: indigenous_area_amazon_biome, conservation_units_amazon_biome, residual_biome. Cache local, nenhuma autenticação. Não precisa refazer a cobertura florestal ou a grade. Não execute processamento com servidor ativo. O painel territorial independe do módulo ambiental; a comparação PPML exige cobertura ambiental para todos os anos selecionados (por exemplo 2020–2024).

## Método

União de polígonos por célula em LAEA e partição mutuamente exclusiva: somente TI, somente UC, ambas, fora de TI/UC mapeadas. Interseção dos incrementos PRODES com as quatro zonas, validação de conservação de área por ano. Denominadores territoriais incluem não floresta; distância por centroide de célula, não distância de cada fragmento. Comparação por faixa agrega hectares-ano. Não confundir fora de TI/UC com área sem nenhuma proteção legal. Limites do snapshot atual, sem datas de vigência, não demonstram efeito causal da proteção. Residual tem área e mapa próprios, não entra nos incrementos anuais, no proxy histórico ou no modelo.

PPML M1 viário vs M2 viário + frações exclusivas TI/UC/ambas por 10 pontos percentuais, na mesma amostra, offset de disponibilidade florestal anterior, efeitos fixos anuais e erros agrupados em blocos de 50 km. Frações são territoriais, não florestais; não é modelo causal nem regressão espacial estrutural. Deviance compara ajuste interno, não validação preditiva. Frações constantes omitidas. Teste sintético inclui sobreposição e conservação de área. Execução real ainda precisa ser validada.

Produtos em data/v3/5/territory; manifesto registra fontes e hashes da grade e incrementos. Mapa usa geometrias simplificadas exclusivamente para renderização. API e CSV disponíveis no painel.
