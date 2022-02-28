# projetoEnem
# ETL para estudos usando os dados do enem2019
- Dados extraidos a partir de microdados_enem201.
- Mulheres nordestinas com idade entre 16 e 26 anos que estavam gravidas ao fazer a prova do ENEM 2019
- Utilizado Pyspark para extrair e carregar o arquivo CSV devido a grande quantidade de dados
- Ao filtrar as informações necessárias é utilizado pandas para o tratamento dos dados (renomeação de colunas e adequação de nomeclatura dos registros)
- Utilizado a biblioteca Sqlalchemy para a carga dos dados em uma tabela mysql.

