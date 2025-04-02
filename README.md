# README - Evaluarea Implementarii

## 1. Tipul de Paralelizare
- **Procese** (folosind `multiprocessing.Pool`)

## 2. Factorul de Paralelism
Testele au fost efectuate pentru urmatoarele valori ale numarului de procese:
- **1 proces** (fara paralelizare)
- **4 procese**

## 3. Numarul de Mesaje Generat
Testele s-au realizat pentru urmatoarele valori ale numarului total de mesaje:
- **100.000** mesaje
- **1.000.000** mesaje
- **2.000.000** mesaje

Distributia mesajelor:
- **Publicatii**: 30%
- **Subscriptii**: 70%

## 4. Timp de Executie

| Nr. Mesaje | 1 Proces (sec) | 4 Procese (sec) |
|------------|----------|-----------------|
| 100.000    | 2.236    | 1.980           | 
| 1.000.000  | 23.37    | 19.776          |
| 2.000.000  | 45.812   | 41.090          |

## 5. Specificatiile Procesorului

- **Model**: `AMD Ryzen 5 5500U with Radeon Graphics`
- **Numar nuclee**: `6`
- **Logical processors**: `12`
- **Frecventa**: `2.10` GHz
- **Cache L1**: `384` KB
- **Cache L2**: `3.0` MB
- **Cache L3**: `8.0` MB

