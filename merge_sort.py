#!/usr/bin/env python
# -*- coding: utf-8 -*-


from mpi4py import MPI
import numpy as np

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# MERGE SORT - Surušiuoti tekstą
# Kad veiktu kodas reikia tekstas.txt failo

# Rušiavimo algoritmas
def merge_sort(arr):
    if len(arr) > 1: # Tikriname ar dar reikia rušiuoti (jei masyvo dydis yra 1 reiškia viskas surušiuota)

        # Daliname masyva į dvi dalis
        left_arr = arr[:len(arr)//2] 
        right_arr = arr[len(arr)//2:]

        # rekursija, rušiuojame tuos dvi dalis kol negauname išrušiuota masyva
        left = merge_sort(left_arr)
        right = merge_sort(right_arr)
        
        return merge(arr, left_arr, right_arr) # gražina merge operacija

# Merge algoritmas (sujungiami du masyvai)
def merge(arr, left_arr, right_arr):
    
    i = 0 # Kairiojo masyvo indeksas
    j = 0 # Dešiniojo masyvo indeksas
    k = 0 # Surušiuoto masyvo indeksas
    
    # Lyginame elementą iš kairiojo masyvo su elementų iš dešiniojo
    while i < len(left_arr) and j < len(right_arr):
        if left_arr[i] < right_arr[j]:
            arr[k] = left_arr[i] # idėdame mažesni elementą į surušiuota masyva
            i += 1
        else:
            arr[k] = right_arr[j]
            j += 1
        k += 1

    while i < len(left_arr):
        arr[k] = left_arr[i]
        i += 1
        k += 1

    while j < len(right_arr):
        arr[k] = right_arr[j]
        j+= 1
        k+=1

    return arr

def split(a, n): # Funkcija dalinanti masyvą į panašias dalis
    k, m = divmod(len(a), n)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n))

# Pagrindinė rušiavimo funkcija
def parallel_sort_merge():

    # daliname masyva į procesoriaus core'u skaičių
    if rank == 0:
    	with open("tekstas.txt") as f: # nuskaitome duomenys
    	    arr = f.read().splitlines()
    	arr = arr[0].split()
    	
    	data = list(split(arr, size)) # naudojama mūsų split() funkcija, size = procesų skaičius.
    else:
     	data = None
    
    
    data1 = comm.scatter(data, root = 0)
    
    # Rušiuojame
    t = merge_sort(data1)
    # Dabar kiekvienas procesus turi SURUŠIUOTA savo masyvo dalį (pvz [1, 4, 5, 10])
    
    # Sujungiame viska į vieną masyvą
    data = comm.gather(t, root = 0)
    # Dabar data atrodo taip [[1, 2, 7], [5, 8, 10], [1, 16, 22]...]

    # Merge funkcija jau surušiuotiems dalims
    if rank == 0:
        while len(data) > 1: 
            if len(data) % 2 == 0: # Tikriname ar procesu skaičius yra lyginis 
                for t in range(0, len(data), 2): # einame per masyvo dalis poromis
                    # Sukuriamas tusčias masyvas, su norimu dydžiu
                    empty_list = [None] * (len(data[t]) + len(data[t+1]))
                    # Sujungiami ir išrušiuojami tos poros
                    data[t] = list(merge(empty_list, data[t], data[t+1])) 
                    data[t+1] = [] 
                data = list(filter(None, data)) # ištriname tusčius masyvus
            else:
                # ta pati darome ir kai turime nelygini skaičiu procesu, tik paskutine masyvo dalį
                # reikės sujungti su priešpaskutinė, kadangi paskutinė dalys neturės poros.
                for t in range(0, len(data)-1, 2):
                    empty_list = [None] * (len(data[t]) + len(data[t+1]))
                    data[t] = list(merge(empty_list, data[t], data[t+1]))
                    data[t+1] = []
                data = list(filter(None, data))
                empty_list_odd = [None] * (len(data[len(data)-2]) + len(data[len(data)-1]))
                data[len(data)-2] = list(merge(empty_list_odd, data[len(data)-2], data[len(data)-1]))
                data[len(data)-1] = []
                data = list(filter(None, data))
        
        print("Surušiuotas masyvas")  
        print(data)


parallel_sort_merge()


