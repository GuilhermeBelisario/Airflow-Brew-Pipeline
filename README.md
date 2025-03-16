Esse repositório é um desafio proposto para fazer um pipeline de dados que:

Consome dados da API Open Brewery DB.

Transforma e armazena em um data lake com arquitetura de medalhão (bronze, prata, ouro).

Usa Python, orquestração e testes.

Inclui monitoramento/alertas, documentação e opcionalmente containerização.

# O que eu planejei é:

## Arquitetura e Ferramentas Utilizadas

    Azure Data Factory (ADF): Responsável pela extração dos dados da API e ingestão inicial na Landing Zone.

    Azure Data Lake Storage (ADLS) Gen 2: Armazenamento principal dos dados em diferentes estágios (Bronze, Silver, Gold).

    PySpark: Utilizado para transformações e enriquecimento dos dados.

    Parquet: Formato de arquivo otimizado para armazenamento e consulta.

    AirFlow: Orquestração do pipeline e agendamento das tarefas.

## Etapas do Processo

### Extração e Landing Zone

Utilizamos o Azure Data Factory para extrair dados de uma API e armazená-los em um container do ADLS Gen 2 no formato JSON. Essa etapa representa a Landing Zone, onde os dados brutos são armazenados sem nenhum tratamento.

### Bronze Layer

    Na camada Bronze, os dados são lidos utilizando PySpark a partir da Landing Zone.
    1.1. Mantemos o formato JSON, mas adicionamos metadados úteis, como:
    1.2. Origem dos dados
    1.3. Data de processamento
    1.4. Outras informações relevantes para rastreabilidade.
    1.5. Os dados enriquecidos são armazenados novamente no ADLS Gen 2, ainda no formato JSON.

### Silver Layer
    
    Na camada Silver, realizamos uma limpeza e seleção das colunas de interesse.

    Os dados são transformados e armazenados no formato Parquet, que é mais eficiente para consultas e análises.

    Essa etapa garante que apenas os dados relevantes e tratados sejam mantidos.

### Gold Layer
    Na camada Gold, otimizamos o schema dos dados e criamos um Star Schema para facilitar a análise e a criação de dashboards.

    Os dados são organizados em tabelas dimensionais e fatos, prontos para serem consumidos por ferramentas de visualização.

    O resultado final é armazenado no ADLS Gen 2 no formato Parquet.

### Orquestração com AirFlow

    Todo o processo é orquestrado e agendado utilizando o AirFlow, garantindo a execução automatizada e a monitoração das tarefas.

### Estrutura de Armazenamento no ADLS Gen 2

    Cada camada da Medallion Architecture é armazenada em containers separados no ADLS Gen 2:

    Landing Zone: Armazena os dados brutos extraídos da API.

    Bronze: Dados enriquecidos com metadados.

    Silver: Dados tratados e filtrados.

    Gold: Dados otimizados para análise.