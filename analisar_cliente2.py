#!/usr/bin/env python3

import sys
import os
import time
import signal
import pickle
from multiprocessing import Process, Value, Semaphore, Lock, Array

# Configurações iniciais
MAX_SIZE_Buffer = 5  # Tamanho do buffer circular
Sec = 2
Threshold_Price = 1000.0
SPECIAL_DAYS = {29, 30, 31}
LEDGER_FILE = "ledger.bin"

# Semáforos e Locks para controle de acesso ao buffer e recursos
empty_slots = Semaphore(MAX_SIZE_Buffer)  # Espaços vazios no buffer
full_slots = Semaphore(0)  # Itens cheios no buffer
buffer_lock = Lock()  # Protege o acesso ao buffer
write_lock = Lock()  # Protege o acesso ao arquivo binário
R_StockWarehouse = Semaphore(1)  # Recurso de verificação de estoque
R_DeliveryCapacity = Semaphore(1)  # Recurso de capacidade de entrega

# Buffer Circular
buffer = Array('d', [0.0] * MAX_SIZE_Buffer)
writes_index = Value('i', 0)
reads_index = Value('i', 0)

def prefix(nome, who):
    """Gera um prefixo formatado com o nome e string de identificação."""
    return f"[{nome}:{who}]"

def parse_line(parts):
    """Analisa uma linha de dados extraída de um arquivo CSV."""
    if len(parts) < 4:
        return None
    data = parts[0].strip()
    produto = parts[2].strip()
    try:
        preco = float(parts[3].strip())
    except ValueError:
        print(f"Erro na conversão do preço para :{parts[3].strip()}")
        return None
    return data, produto, preco

def write_ledger(data):
    """Escreve os dados no ledger binário."""
    with write_lock:
        with open(LEDGER_FILE, 'ab') as f:
            pickle.dump(data, f)

def P_expensive(nome, total):
    """Processo para produtos caros (> 1000€)."""
    while True:
        full_slots.acquire()
        with buffer_lock:
            preco = buffer[reads_index.value]
            print(f"{prefix(nome, 'P_Expensive')} Verificando preço: {preco:.2f}€")
            with total.get_lock():
                total.value += preco
            if preco > Threshold_Price:
                print(f"{prefix(nome, 'P_Expensive')} Produto com preço elevado: {preco:.2f}€")
                data = {"nome": "Produto", "price": preco, "expensive": True}
                write_ledger(data)
            reads_index.value = (reads_index.value + 1) % MAX_SIZE_Buffer
        empty_slots.release()
        time.sleep(Sec)

def P_special_days(nome):
    """Processo para compras em datas especiais (29, 30, 31)."""
    while True:
        full_slots.acquire()
        with buffer_lock:
            preco = buffer[reads_index.value]
            data = {"nome": "Produto", "price": preco, "special": True}
            write_ledger(data)
            reads_index.value = (reads_index.value + 1) % MAX_SIZE_Buffer
        empty_slots.release()
        time.sleep(Sec)

def P_Total(nome, total):
    """Processo para calcular o total acumulado."""
    while True:
        time.sleep(Sec)
        with buffer_lock:
            print(f"{prefix(nome, 'P_Total')} Total acumulado: {total.value:.2f}€")

def signal_handler(sig, frame):
    """Signal handler para encerrar o processo de análise do cliente."""
    print("\nEncerrar o processo de análise do cliente.")
    sys.exit(0)

def main():
    """Função principal que gerencia os processos e controla o fluxo de dados."""
    global total
    total = Value('d', 0.0)

    # Configuração do timer para saída periódica
    signal.signal(signal.SIGALRM, signal_handler)
    signal.setitimer(signal.ITIMER_REAL, 3, 3)

    nome = "ClienteXYZ"
    
    # Iniciando os processos consumidores
    p1 = Process(target=P_expensive, args=(nome, total))
    p2 = Process(target=P_special_days, args=(nome,))
    p3 = Process(target=P_Total, args=(nome, total))

    p1.start()
    p2.start()
    p3.start()

    # Leitura do arquivo CSV
    if len(sys.argv) != 2:
        print("Uso: python3 analisar_cliente.py <caminho_para_o_csv>")
        sys.exit(1)

    ficheiro = sys.argv[1]
    if not os.path.isfile(ficheiro):
        print(f"Ficheiro {ficheiro} não encontrado.")
        sys.exit(1)

    with open(ficheiro, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.strip().split(',')
            parsed = parse_line(parts)
            if parsed:
                # Coloca os itens no buffer
                empty_slots.acquire()
                with buffer_lock:
                    buffer[writes_index.value] = parsed[2]  # Coloca o preço no buffer
                    writes_index.value = (writes_index.value + 1) % MAX_SIZE_Buffer
                full_slots.release()
            time.sleep(Sec)

    # Encerra os processos após o processamento dos dados
    p1.join()
    p2.join()
    p3.join()

if __name__ == "__main__":
    main()
