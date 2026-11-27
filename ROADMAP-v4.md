# GeoForest Lab — planejamento V4

Preservar V3 funcional. Objetivo: comunicar conhecimento ambiental a cidadãos, estudantes e pesquisadores, não apenas disponibilizar indicadores.

## Experiência e transparência
- Organizar páginas por perguntas de investigação, com linguagem acessível, síntese interpretativa baseada nos resultados reais, distinção entre evidência e hipótese.
- Cada indicador, mapa, gráfico e tabela deve explicar significado, unidade, cálculo, fonte, período, cobertura, limitações e o que não permite concluir. Links oficiais e detalhamento científico expansível.
- Modos exploratório e científico sobre os mesmos dados; não ocultar limitações essenciais no modo acessível.
- Paletas semânticas: verde para disponibilidade florestal, tons quentes para incremento, cinza para ausência de dados; intervalos exclusivos, ano explícito e bordas discretas.
- Separar floresta observada, proxy de elegibilidade PRODES, não floresta natural e desmatamento histórico. Não confundir baixa disponibilidade com desmatamento.
- Explorador por município, célula e polígono personalizado, com perfil territorial, camadas independentes e exportações.
- Central de transparência: fontes, datas, versões, hashes, resolução, cobertura, lacunas, qualidade, métodos, referências e reprodução. Estados explícitos de indisponibilidade; não preencher ausências com zero.
- Navegação por perguntas: panorama, explorador, desmatamento, estradas, fogo, contexto socioeconômico, dinâmica florestal, análises científicas e transparência.

## Novas fontes e validação
- Priorizar MapBiomas via Earth Engine agora que o usuário informou ter registro comunitário; manter processamento local e alternativa PRODES sem autenticação.
- Verificar acesso real, asset, bandas, classes, período e termos de uso antes de integração. Não misturar estimativas PRODES e observações MapBiomas; cache e proveniência separados.
- Investigar BDQueimadas (focos não equivalem a incêndios ou área queimada), SIDRA/IBGE (contexto municipal), Copernicus DEM (relevo), DETER (alertas não equivalem a taxa PRODES). Validar endpoints antes de tornar fontes obrigatórias.
- TI/UC e residual permanecem opcionais; não bloquear por indisponibilidade.
- Validar Moran/LISA externamente, diagnóstico residual, validação espacial e sensibilidade de resolução, modelos e denominadores. Sem inferência causal a partir da rede viária contemporânea.
- Exportar figuras, dados, parâmetros e relatórios reproduzíveis.

## Ordem de trabalho
1. Validar acesso Earth Engine e MapBiomas em ambiente real.
2. Criar branch V4 a partir da V3 funcional; corrigir comunicação e cartografia.
3. Integrar fontes verificadas progressivamente, explorador territorial e central de transparência.
4. Validar métodos, testes de integração, build e experiência de navegação.

Registro de planejamento, não implementação. Nenhuma credencial deve ser versionada. Não exigir serviços pagos nem downloads manuais.
