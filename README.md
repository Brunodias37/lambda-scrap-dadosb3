# 📈 Coleta e Armazenamento de Dados da B3

Este projeto é uma função AWS Lambda responsável por coletar informações de portfólio diário da B3, processá-las e armazená-las em formato Parquet no Amazon S3. Ele também registra os dados no AWS Glue e repara a tabela usando o Athena.

---
## 🧩 Layer Lambda Personalizado

Este projeto utiliza um **Lambda Layer** para incluir dependências externas como `pandas`, `pyarrow`, `s3fs`, entre outras bibliotecas necessárias para o funcionamento da função.

🔗 **Download do Layer**:  
👉 [Clique aqui para baixar o layer utilizado](https://drive.google.com/file/d/1WEEQALPhkLr4gmpK0PY1W5E4XPYkKdox/view?usp=sharing)

### 📦 Conteúdo do Layer

O layer contém as seguintes dependências:
- `requests`
- `pandas`
- `s3fs`
- `pyarrow`
- `boto3`

💡 **Dica:** Ao criar sua função Lambda, certifique-se de:
- Associar este layer à função.
- Usar um runtime compatível (por exemplo, Python 3.9).
- Verificar se o tamanho total (código + layer) está dentro do limite da AWS.


## 🚀 Visão Geral do Processo

1. 🔗 **Coleta dos dados** via requisição à API da B3  
2. 🧹 **Processamento** e limpeza dos registros  
3. 📦 **Criação de DataFrame** com pandas  
4. 🗂️ **Armazenamento em S3** no formato Parquet com partição  
5. 📊 **Criação/Atualização da tabela no Glue Catalog**  
6. 🛠️ **Reparo da tabela via Athena** para leitura por SQL

---

## 🛠️ Tecnologias Utilizadas

- **Python 3**
- `requests`, `pandas`, `json`, `datetime`
- `boto3` (AWS SDK)
- `s3fs`, `pyarrow`
- AWS Lambda, S3, Glue, Athena

---

## 📁 Estrutura de Arquivos

- `salvar_dataframe_parquet_s3`: salva os dados em S3  
- `process_registro`: limpa e organiza cada registro  
- `criar_tabela_raw`: cria ou atualiza a tabela Glue  
- `reparar_tabela_athena`: sincroniza partições da tabela no Athena  
- `lambda_handler`: função principal executada pelo Lambda

---

## 📌 Pré-requisitos

- Conta AWS com permissões para Lambda, S3, Glue e Athena  
- Bucket S3 existente: `bovespa-{id_conta_aws}`  
- Pasta: `raw/tb_dados_b3_raw`  
- Resultado das queries do Athena: `s3://bovespa-{id_conta_aws}/athena-query-results/`

---

## ⚙️ Configuração do Ambiente

Certifique-se de que os seguintes pacotes estejam instalados:

```bash
pip install requests pandas s3fs boto3 pyarrow
```

---

## 🧪 Executando Localmente

Você pode testar localmente simulando a execução da função `lambda_handler`:

```python
lambda_handler(None, None)
```

---

## 📊 Exemplo de Saída

```json
{
  "statusCode": 200,
  "body": "Arquivo com dados de 2025/07/29 salvo com sucesso!"
}
```

---

## 📎 Observações

- As listas `segment_list` e `type_list` são utilizadas durante o processamento, mas não são mantidas no arquivo final Parquet.  
- A tabela criada no Glue é do tipo EXTERNAL e particionada por `anomesdia`.

---

## 🤝 Contribuições

Sugestões de melhorias são bem-vindas! Basta abrir uma issue ou pull request 😄

---

Se quiser que eu transforme isso num projeto com estrutura de arquivos e até crie exemplos de consulta com Athena, posso te ajudar com isso também! 💡
