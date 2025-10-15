#!/bin/bash

# Verificar argumentos
if [ $# -ne 2 ]; then
    echo "Uso: $0 <pasta_dos_clientes> <max_processos>"
    exit 1
fi

echo "Analises começando!"

#...
dir_clientes=$1
max=$2
let i=0

processo_py(){
python3 analisar_cliente.py $1
}


for ficheiro in $dir_clientes/*.csv; 
do

echo "Pesquisa: $ficheiro"
#cat $ficheiro &
processo_py $ficheiro
(echo ola && sleep 2 && echo adeus)&

((i++))

if [ $i -ge $max ]; then
   jobs
   wait -n
  ((i--))
   fi
done
wait


echo "Todas as análises concluídas."
