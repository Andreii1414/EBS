# README - Evaluarea Implementarii

Realizati o evaluare a sistemului, masurand pentru inregistrarea a 10000 de subscriptii simple, urmatoarele statistici: a) cate publicatii se livreaza cu succes prin reteaua de brokeri intr-un interval continuu de feed de 3 minute, b) latenta medie de livrare a unei publicatii (timpul de la emitere pana la primire) pentru publicatiile trimise in acelasi interval, c) rata de potrivire (matching) pentru cazul in care subscriptiile generate contin pe unul dintre campuri doar operator de egalitate (100%) comparata cu situatia in care frecventa operatorului de egalitate pe campul respectiv este aproximativ un sfert (25%). Redactati un scurt raport de evaluare a solutiei.
![diagram (1)](https://github.com/user-attachments/assets/bdb02f4f-bac9-4030-b2df-7492fb431a98)


!!! Toate testele le-am facut folosind un delay de o secunda la trimiterea publicatiilor si la trimiterea subscriptiilor catre brokeri.

#### a) 
        Sub 1: 124 notificari 
        Sub 2: 153 notificari
        Sub 3: 162 notificari
        Total: 439 notificari (delay o secunda)

#### b)
Am adaugat un timestamp in specificatia .protobuf (momentul in care a fost 
trimisa publicatia). Am calculat timestamp_curent – timestamp_trimitere pentru 
fiecare notificare primita. 
Latenta medie: aproximativ 1.5ms (local)

#### c)
Cu 100% (=) pe campul temp: 117+118+95 notificari primite de cei 3 subscriberi. In 3 minute se trimit 180 de notificari (*3 pentru ca sunt 3 subscriberi). Deci (330/540)*100 = 61% notificari primite

Cu 25% (=) pe campul temp: ((154+150+146)/(3*180))*100 = (450/540)*100 = 83%
