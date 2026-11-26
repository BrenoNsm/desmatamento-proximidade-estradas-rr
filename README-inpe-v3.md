# Disponibilidade florestal — alternativa local INPE

Sem autenticação, Earth Engine ou Google Cloud. Reutiliza a V2/V3.

```bash
git pull origin develop/v3
source .venv/bin/activate
python -m pytest -q
python -u environment_inpe_v3.py --list-layers
python -u environment_inpe_v3.py --cell-km 5 --start 2020 --end 2024 --check-only
python -u environment_inpe_v3.py --cell-km 5 --start 2020 --end 2024
python -m uvicorn api_v3:app --host 127.0.0.1 --port 8000
```

A descoberta consulta GetCapabilities do mesmo WFS utilizado na V2. Se os aliases não coincidirem com os nomes publicados, o programa interrompe e lista as camadas. Parâmetros --baseline, --nonforest, --water, --biome aceitam nomes exatos do catálogo; não selecionar camadas sem validar semântica. O baseline precisa ter year=2007 ou class_name=d2007. Não se comprovou ainda que o serviço fornece todas essas quatro camadas ou o atributo esperado; a integração depende dessa verificação. --check-only baixa/cacheia as quatro camadas e verifica o baseline, mas não reconstrói as áreas.

A disponibilidade florestal é um proxy: domínio do bioma menos união de não floresta, água, desmatamento pré-2008 e incrementos anteriores desde 2008. Não considera regeneração nem máscaras históricas de resíduos e nuvens. Anos ausentes interrompem a reconstrução. Observações com incremento maior que o proxy são excluídas por cobertura válida zero e contabilizadas. Os resultados ambientais substituem o arquivo environment.parquet da mesma grade, sem misturar fontes. O painel e modelo são reutilizados, com indicação do provedor. A inferência PPML continua exploratória e exige validação metodológica.

Não execute simultaneamente com o servidor. Os módulos MapBiomas e GEE anteriores permanecem opcionais, não são necessários para iniciar a aplicação. Não há integração de relevo ou áreas protegidas nesta etapa.
