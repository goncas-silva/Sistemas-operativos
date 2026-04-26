# ShoppingSO

Sistema compacto para análise de compras de clientes em grande escala

---

## Descrição

O **ShoppingSO** demonstra conceitos fundamentais de Sistemas Operativos, nomeadamente concorrência, paralelismo, sincronização e comunicação entre processos e threads. O sistema processa ficheiros CSV de clientes com milhares de registos de compras, dividindo o trabalho entre processos e threads de forma coordenada.

---

## Estrutura do Projeto

```
SO-TI-XX/
├── executar_analises.sh   # Script Bash — gestão de processos
├── analisar_cliente.py    # Programa Python — análise por cliente
└── csvs/                  # Pasta com ficheiros CSV dos clientes
```

---

## Fase 1 — Processos e Threads

### Arquitetura

- **Script Bash** (`executar_analises.sh`): lança um processo Python por cada ficheiro CSV, garantindo que apenas `N` processos correm em simultâneo.
- **Programa Python** (`analisar_cliente.py`): cada processo cria 3 threads que analisam o ficheiro em paralelo via filas partilhadas (tamanho 5).

### Threads

| Thread | Função |
|--------|--------|
| T1 | Identifica compras com valor superior a **1000.00 €** |
| T2 | Calcula o **valor total** gasto pelo cliente |
| T3 | Identifica compras realizadas nos dias **29, 30 e 31** de qualquer mês |

### Utilização

```bash
./executar_analises.sh <pasta_dos_clientes> <max_processos>
```

```bash
python3 analisar_cliente.py <caminho_ficheiro_csv>
```

### Formato do CSV

```
Data,Hora,Produto,Preco
2026-04-23,23:36:06,PlacaGrafica-Amazon-VHD528,361.51
2026-03-27,04:05:20,Carregador-Intel-HWD278,421.43
2026-04-30,15:18:14,Rato-Xiaomi-KDF814,1395.13
```

### Exemplo de Output

```
[customer1:main] Análise iniciada.
[customer1:T1] Compra cara: Rato-Xiaomi-KDF814 -> 1395.13€
[customer1:T3] Compra dia especial: Rato-Xiaomi-KDF814 -> 2026-04-30
[customer1:T2] Total gasto: 136894.46€
[customer1:main] Análise concluída.
```

> Todas as mensagens são prefixadas por `[NomeFicheiro:Thread]`.

---

## Fase 2 — Buffer Circular, Semáforos e Deadlock

### Arquitetura

A Fase 2 substitui as 3 threads por **3 processos consumidores** e as filas independentes por um **buffer circular partilhado**.

| Componente | Função |
|------------|--------|
| `Main` | Lê o CSV linha a linha e produz dados no buffer |
| `P_Expensive` | Processa compras com preço > 1000 € |
| `P_Special` | Processa compras nos dias 29, 30 e 31 |
| `P_Total` | Acumula o total e imprime periodicamente |

### Sincronização

- **Buffer circular** com controlo de ocupação via semáforos (modelo produtor-consumidor).
- **Mutex** para acesso exclusivo ao buffer.
- Cada posição do buffer é libertada apenas após ser lida pelos **3 consumidores**.

### Recursos Partilhados e Deadlock

| Recurso | Identificador | Função |
|---------|--------------|--------|
| Verificação de stock | `R_StockWarehouse` | Confirma disponibilidade em armazém |
| Capacidade de entrega | `R_DeliveryCapacity` | Verifica disponibilidade logística |

- `P_Expensive` adquire: `R_StockWarehouse` → `R_DeliveryCapacity`
- `P_Special` adquire: `R_DeliveryCapacity` → `R_StockWarehouse`

> A inversão na ordem de aquisição cria uma possibilidade real de **deadlock** por espera circular.

### Deteção de Estado Seguro

Os processos `P_Expensive` e `P_Special` imprimem o estado dos recursos e indicam se o sistema se encontra em `[SAFE STATE]` ou `[UNSAFE STATE]`.

### Output Periódico

O processo `P_Total` usa `SIGALRM` para imprimir o total acumulado a cada **3 segundos**.

### Registo Binário

Cada transação processada por `P_Expensive` ou `P_Special` é guardada em `ledger.bin`:

```python
# P_Expensive
{"nome": <produto>, "price": <preço>, "expensive": True/False}

# P_Special
{"nome": <produto>, "price": <preço>, "special": True/False}
```

> O acesso ao ficheiro é protegido por `Lock` para evitar corrupção em escritas concorrentes.

---


- **Bash** — gestão de processos e controlo de concorrência
- **Python 3** — threads, processos, semáforos, mutex, memória partilhada, sinais

---



