#!/usr/bin/env python3

# imports:
import sys
import os
import time
from multiprocessing import Process, Queue, Value

SLEEP_SEC=0.010
QUEUE_SIZE=5
PRICE_THRESHOLD= 1000.0
SPECIAL_DAYS= {29, 30, 31}

def prefix (name, who):


    return f"[{name}:{who}]"


def parse_line(parts):
    if len(parts)<4:
        return None

    data = parts[0].strip()
    produto = parts[2].strip()

    try:
        preco= float(parts[3].strip())
    except ValueError:
        print(f"Erro na conversão do preço para :{parts[3].strip()}")
        return None

    return data, produto, preco
    


# Thread 1: artigos caros (>1000€)
def thread1_artigos_caros(name,q):
    while True:
        item = q.get()
        if item is None:
            break
        data, produto, preco = item
        if preco > PRICE_THRESHOLD:
            print(f"{prefix(name, 'P1')} Compra cara: {produto} -> {preco:.2f}€")
        time.sleep(SLEEP_SEC)

    

    

# Thread 2: total gasto
def thread2_total_gasto(name,q,result):
    total=0.0
    while True:
        item=q.get()
        if item is None:
            break
        data, produto, preco = item
        total += preco
        time.sleep(SLEEP_SEC)
    result.value=total
    print(f"{prefix(name, 'P2')} Total gasto: {total:.2f}€")
    

# Thread 3: compras nos dias 29,30,31
def thread3_analise_temporal(name,q):
    while True:
        item = q.get()
        if item is None:
            break
        data, produto, preco = item
        try:
            day = int(data.split('-')[2])
            if day in SPECIAL_DAYS:
                print(f"{prefix(name, 'P3')} Compra dia especial: {produto} -> {data}")
        except Exception:
            print(f"{prefix(name, 'P3')} Erro ao processar data: {data}")  # Log para erro de formatação de data
        time.sleep(SLEEP_SEC)

    



# -- Main thread --
def main ():




    if len(sys.argv) !=2:
        print("Para usar é necessario: python3 analisar_cliente <caminho_ficheiro_csv>")
        sys.exit(1)

    
    ficheiro=sys.argv[1] #sys.argv[1] é o caminho para o arquivo csvs

    if not os.path.isfile(ficheiro):
        print(f"ficheiro: {ficheiro}, não foi encontrado ")
        sys.exit(1)

    nome,extensao = os.path.splittext(ficheiro)
    if extensao.lower() != '.csv':
        print(f"Erro o arquivo {ficheiro} não tem a extensão .csv")
        sys.exit(1)

    q1=Queue(QUEUE_SIZE)
    q2=Queue(QUEUE_SIZE)
    q3=Queue(QUEUE_SIZE)

    resultado=Value('d', 0.0) #armazena o total

    p1 = Process(target=process_artigos_caros, args=(name, q1))
    p2 = Process(target=process_total_gasto, args=(name, q2, result))
    p3 = Process(target=process_analise_temporal, args=(name, q3))

    print(f"{prefix(name, 'main')} Análise iniciada.")

    p1.start()
    p2.start()
    p3.start()

    with open(ficheiro, 'r', encoding='utf-8', newline='') as fh:
        first = True
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = [p for p in line.split(',')]
            if first:
                first = False
                if parts and parts[0].lower().startswith('data'):
                    continue  # Ignora a primeira linha (cabeçalho)
            parsed = parse_line(parts)
            if parsed:
                # Coloca os itens nas três filas
                q1.put(parsed)
                q2.put(parsed)
                q3.put(parsed)
            else:
                print(f"{prefix(name, 'main')} Linha inválida: {line}")  # Log para linhas inválidas


    q1.put(None)
    q2.put(None)
    q3.put(None)


    p1.join()
    p2.join()
    p3.join()

    print(f"{prefix(name, 'main')} Análise concluída.")
    print(f"{prefix(name, 'main')} Total gasto: {result.value:.2f}€")

if __name__ == '__main__':
    main()
    












     
        


    


    
    
# Análise iniciada.

# ....

# Análise concluída.
