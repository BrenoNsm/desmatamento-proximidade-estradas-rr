# Cobertura histórica e modelo exploratório

Após gerar a grade V3, execute:

```bash
git pull origin develop/v3
source .venv/bin/activate
python -m pip install -r requirements-v3-environment.txt
python -m pytest -q
python -u environment_v3.py --cell-km 5 --start 2020 --end 2024
python -m uvicorn api_v3:app --host 127.0.0.1 --port 8000
```

Selecione 2020–2024 (ou os anos efetivamente disponíveis) na interface e clique em Analisar cobertura e Estimar modelo PPML. Para todos os anos disponíveis: python -u environment_v3.py --cell-km 5. A leitura remota usa janelas reprojetadas a 30 m em LAEA e cache por ano e hash da grade; não precisa baixar manualmente rasters. Pode consumir banda e tempo consideráveis. O endereço remoto, as classes da coleção e o período disponível precisam de validação contra a documentação oficial e execução local; não se considera a integração validada. Se o servidor falhar, o módulo interrompe, mantendo os anos concluídos em cache. 

O denominador é a cobertura florestal MapBiomas t-1, não a área efetivamente elegível segundo PRODES. Períodos de referência e classificações diferem. Vegetação aberta é armazenada separadamente, não adicionada ao denominador florestal. Classificação por centro de pixel, aproximação de bordas. Exclui observações com menos de 95% de cobertura válida ou menos de 1 ha florestal; informa incremento excluído. A razão por faixa agrega hectares-ano, não representa taxa acumulada. PPML usa offset log(forest_ha), log1p(distância), log1p(densidade), efeitos anuais e erros agrupados em blocos de 50 km. Razões de médias por unidade da variável transformada, não por km. Não é regressão espacial estrutural, não estabelece causalidade. Ainda faltam covariáveis ambientais, diagnóstico residual espacial, validação cruzada e comparação de modelos.

Fonte: https://brasil.mapbiomas.org/ e portal de downloads da coleção. Valide os códigos de classes e a versão antes de publicar. 
