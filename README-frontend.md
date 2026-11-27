# GeoForest Lab 3.1 — React + TypeScript

Sem necessidade de baixar as camadas territoriais, sem Google Cloud, sem Streamlit.
Usa a grade e disponibilidade florestal já processadas.

## Debian

Requer Node.js 20 ou 22 LTS (preferir 22), npm, Python e venv existentes. Verifique node -v e npm -v. Se não possuir Node, instale uma versão LTS seguindo https://nodejs.org/en/download ou o gerenciador de pacotes do Debian, desde que a versão seja compatível.

Na raiz:
```bash
git pull origin develop/v3
source .venv/bin/activate
python -m pytest -q
cd frontend
npm install
npm run build
cd ..
python -m uvicorn api_v3:app --host 127.0.0.1 --port 8000
```
Abra http://127.0.0.1:8000. O backend serve o build estático; não é necessário manter dois processos para uso normal. Reinicie a API após o build. Se o build ainda não existir, a API mantém a interface HTML legada.

## Desenvolvimento

Terminal 1 na raiz: python -m uvicorn api_v3:app --host 127.0.0.1 --port 8000
Terminal 2: cd frontend && npm run dev
Abra http://127.0.0.1:5173. Vite encaminha /api à API local.

## Telas

Visão geral, exploração espacial MapLibre, dinâmica temporal, matriz espaço-temporal, Moran/LISA, disponibilidade florestal, PPML, metodologia, manifesto e exportação CSV. Escolha 2020–2024 para cobertura ambiental já processada. Módulos territoriais ficam opcionais no backend, sem controles no React. O mapa base depende de internet (CARTO); as geometrias e análises são locais. O frontend usa Recharts; exportação de figuras SVG e edição de áreas ainda não implementadas. Build e experiência visual precisam ser validados no ambiente local; nenhuma nova dependência Python requerida.
