# SAGeo

Modelo de Simulação de Bases de Dados Geo-replicadas Configurável

<br/>

## Estrutura do Repositório

Este repositório está dividido em 4 diretorias:

-   _inputs_ - Nesta diretoria é possível encontrar os ficheiros de input para a execução do modelo
-   _models_ - Nesta diretoria é possível encontrar as diversas configurações do modelo de simulação
-   _results_ - Nesta diretoria é possível encontrar os resultados da execução do modelo
-   _tests_ - Nesta diretoria é possível encontrar os scripts para a execução do modelo

Além disso, o ficheiro `main.py` contém o código responsável por executar os modelos e apresentar os resultados e o ficheiro `requirements.txt` contém as dependências do programa

<br/>

## Instalação de dependências

Para instalar as dependências do modelo basta executar:

`pip3 install -r requirements.txt`

<br/>

## Execução do modelo

Para executar o modelo, primeiramente, cria-se um novo ficheiro de script na pasta _tests_, como por exemplo, `example.sh`. Posteriormente, neste ficheiro, invoca-se a função `main.py`, seguido dos ficheiros de input desejados e, por último, o nome do ficheiro onde será guardado o resultado.

Seguindo o exemplo do teste de validação do sistema Wren, que podemos encontrar na diretoria _tests_, no ficheiro `validation.sh`, temos:

`python3 ../main.py ../inputs/validation_5050_cache.json ../inputs/validation_5050_nocache.json ../results/validation_5050.png`

Por último, executamos o ficheiro de script:

`sh example.sh`
