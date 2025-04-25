# 🚍 ETL - Dados da API Olho Vivo (SPTrans)

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) sobre os dados públicos da **API Olho Vivo**, disponibilizados pela SPTrans, com foco em dados de posição e velocidade de ônibus da cidade de São Paulo. Foi desenvolvido como trabalho final do MBA em Engenharia de Dados, visando fornecer datasets confiáveis e acessíveis para análises sobre mobilidade urbana.

## 📂 Estrutura do Projeto

O projeto está dividido em três scripts principais:

### 1. `get-bus-data.py` - Ingestão Bruta
Responsável por consumir os dados diretamente da API Olho Vivo e armazená-los em formato **JSON cru** para posterior transformação.

### 2. `etl-olho-vivo-ingestao-posicoes.py` - Transformação e Armazenamento
Realiza o **flattening** dos dados JSON brutos, padronizando-os em **formato Parquet** e enviando-os para um **bucket S3**. É executado diariamente às **6:00 AM (GMT-3)** via **AWS EventBridge**.

### 3. `etl-olho-vivo-velocidades-medias.py` - Agregação e Geração de Datasets
Consolida os dados processados anteriormente, gerando arquivos **CSV** com agregações úteis para análise, como:

- Velocidade média por trajeto
- Pontos de lentidão identificados
- Localização de veículos com/sem acessibilidade a cada 30 minutos

Executado automaticamente às **6:30 AM (GMT-3)** via EventBridge.

## 🧼 Processos de Limpeza Aplicados

Durante o ETL, os seguintes filtros são aplicados para garantir a qualidade dos dados:

- 🔻 **Intervalos > 10 minutos** entre aquisições são descartados
- 📏 **Distâncias arredondadas** a duas casas decimais (centímetros)
- 🚫 Tuplas com **posição anterior nula** são ignoradas
- 🚀 **Velocidades > 120 km/h** são desconsideradas como anomalias

## 📊 Resultados e Produtos Gerados

- Datasets limpos e agregados prontos para análise
- Três principais tabelas derivadas:
  - Velocidades médias
  - Pontos de lentidão
  - Localização com acessibilidade

## ☁️ Armazenamento e Acesso

- Os dados completos estão armazenados na **AWS S3 Glacier**, por questões de custo.
- Amostras estão disponíveis no **Kaggle**:
  👉 [kaggle.com/datasets/jonasmarma/samples-bus-data-from-sao-paulo](https://www.kaggle.com/datasets/jonasmarma/samples-bus-data-from-sao-paulo)

Para obter acesso completo aos dados, entre em contato via email: jonas.marma@gmail.com

## 📈 Próximos Passos

- Melhorar a **documentação** e acessibilidade dos dados
- Criar um **template de deploy rápido via CloudFormation**
- Desenvolver **dashboards no Looker Studio** para visualização interativa
- Incentivar **colaborações e feedbacks** da comunidade

## 🧠 Insights e Considerações

Este projeto demonstrou que é possível construir um pipeline robusto, confiável e de baixo custo para análise de dados de mobilidade urbana, mesmo em ambientes com recursos financeiros limitados.

As maiores dificuldades foram:
- Balancear custo e performance
- Garantir a **qualidade dos dados** sem sacrificar a **escalabilidade**
- Armazenar grandes volumes sem comprometer acessibilidade


---

**Contato:**  
Para dúvidas, colaborações ou acesso completo aos dados, abra uma issue ou envie um pull request.

