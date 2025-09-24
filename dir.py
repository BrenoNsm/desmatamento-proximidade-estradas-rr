import os

# Define o caminho do diretório
diretorio = r'C:\Users\bnascimento\Downloads\norte-250922-free.shp'

# Verifica se o caminho é um diretório válido
if os.path.isdir(diretorio):
    # Percorre todos os arquivos e diretórios dentro do caminho especificado
    for nome_arquivo in os.listdir(diretorio):
        # Cria o caminho completo do arquivo
        caminho_completo_arquivo = os.path.join(diretorio, nome_arquivo)

        # Verifica se o item atual é um arquivo (e não uma subpasta)
        if os.path.isfile(caminho_completo_arquivo):
            # Imprime o nome do arquivo
            print(f"Nome do arquivo: {nome_arquivo}")