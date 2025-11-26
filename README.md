# 📘 Projeto de Data Warehouse AdventureWorks com ETL em Airflow

## 📌 1. Introdução

Este projeto implementa um **Data Warehouse (DW)** baseado no conjunto de dados *AdventureWorks*, utilizando um **modelo multidimensional Star Schema**.  
Os dados são carregados através de processos **ETL orquestrados pelo Apache Airflow** e armazenados em um banco **PostgreSQL** dedicado ao DW.

O objetivo é permitir análise histórica de vendas, produtos, clientes e tempo, garantindo organização, performance e fácil expansão.

---

## ⭐ 2. Arquitetura do Projeto

A solução é composta por:

- **Apache Airflow**: orquestração das rotinas ETL  
- **PostgreSQL (DW)**: armazenamento dimensional  
- **CSV AdventureWorks**: base de origem para as dimensões e fato  
- **Docker Compose**: infraestrutura completa (Airflow + PostgreSQL)

### 🔧 Componentes

| Componente | Função |
|-----------|--------|
| `postgres_airflow` | Metadados do Airflow |
| `postgres_dw` | Data Warehouse |
| `webserver`, `scheduler` | DAGs e agendamentos |
| `dags/*.py` | Arquivos ETL |
| `csv/` | Base AdventureWorks em CSV |

---

## 🧩 3. Modelo Multidimensional – Star Schema

O modelo escolhido utiliza **1 tabela fato** e **3 dimensões** principais:

### 🧭 Fato: `FatoVendas`
Contém: quantidade, valor da venda, desconto, custo e chaves das dimensões.

### 🧱 Dimensões
- **DimTempo**  
  Datas de 2010 a 2030, com atributos de ano, trimestre, mês e dia da semana.

- **DimProduto**  
  Informações de produto, categoria e subcategoria.

- **DimCliente**  
  Dados dos clientes: nome, cidade, estado e país.

### 📌 Diagrama Estrela (representação simplificada)

           DimProduto
                |
                |
DimCliente ---- FatoVendas ---- DimTempo

---

## 📊 4. Indicadores (KPIs)

Os indicadores definidos permitem análise completa do desempenho comercial:

1. **Total de Vendas (R$)**
2. **Quantidade Vendida**
3. **Ticket Médio**
4. **Vendas por Produto**
5. **Vendas por Categoria**
6. **Vendas por Cliente**
7. **Vendas por Região (Estado / País)**
8. **Lucro (Venda – Custo)**
9. **Desconto Médio Aplicado**
10. **Crescimento Mensal de Vendas (MoM)**

Exemplo de KPI calculado:

```sql
SELECT SUM(salesamount) AS total_vendas
FROM fatovendas;
O projeto possui as seguintes DAGs:

DAG	                  Função
etl_dimtempo	        Cria calendário de 2010–2030
etl_dimcliente	      Carrega dimensão cliente
etl_dimproduto	      Carrega dimensão produto
etl_fatvendas	        Monta a FatoVendas
create_dw_tables	    Cria as tabelas no DW
