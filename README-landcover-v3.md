# MapBiomas — uso e cobertura da terra

Na mesma branch develop/v3. Requer Earth Engine autenticado, requirements-gee.txt e grade já processada. Não execute environment_gee_v3.py antigo, pois ele substitui environment.parquet. O novo landcover_v3.py grava somente data/v3/5/landcover.

Teste inicial: python -u landcover_v3.py --project SEU_PROJETO --cell-km 5 --start 2023 --end 2024 --batch-size 50

Série inicial: python -u landcover_v3.py --project SEU_PROJETO --cell-km 5 --start 2020 --end 2024 --batch-size 50

O cache é separado por hash da grade, configuração, ano e lote. Se interrompido, repita o mesmo comando. A publicação final contém o período solicitado e a transição entre os extremos, sem apagar o cache. Não execute simultaneamente com a API. Compile frontend com npm run build e reinicie uvicorn. A página Uso e cobertura da terra usa período próprio explicitamente informado, não o filtro anual PRODES.

Grupos analíticos próprios, não hierarquia oficial. Validar a correspondência dos códigos com a legenda da coleção 11 antes de publicação. Códigos desconhecidos e sem classificação separados. Mudanças são comparações pixel a pixel entre extremos; não revelam a sequência intermediária nem causalidade. Não há ainda mapas MapBiomas, fogo, relevo, chuva ou integração ao modelo PPML. Testes sintéticos de parsing e exclusividade dos grupos; build e acesso real ainda precisam ser verificados. O registro comunitário deve ser utilizado conforme os termos e cotas aplicáveis; nenhuma credencial versionada.
