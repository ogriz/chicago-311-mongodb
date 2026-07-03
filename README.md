# Chicago 311 Service Requests — MongoDB analiza

Projekat iz predmeta **Sistemi baza podataka**, FTN Novi Sad, 2026.

**Autori:** Marko Zelić IN48-2022, Uroš Ogrizović IN12-2022

## O projektu

Analiza 4.3 miliona zahteva građana Chicaga (311 servis) koristeći MongoDB aggregation framework.
Projekat pokriva celokupan životni ciklus: učitavanje podataka, normalizovana šema, 10 analitičkih upita,
optimizacija (embedded šema + indeksi), uporedna analiza performansi i vizualizacija u Metabase-u.

## Dataset

Chicago 311 Service Requests (~1.3 GB, 12 CSV fajlova):

**Preuzimanje:** https://www.kaggle.com/datasets/chicago/chicago-311-service-requests

Nakon preuzimanja, raspakovati CSV fajlove u `data/raw/` folder.

## Pokretanje

### Preduslovi

- Python 3.9+
- MongoDB 6.0+ (lokalno, port 27017)
- Docker (za Metabase)

### Instalacija

```bash
pip install -r requirements.txt
```

### Jupyter notebook

```bash
cd notebooks
jupyter notebook Chicago311Analysis.ipynb
```

Notebook se pokreće sekvencijalno — svaka ćelija zavisi od prethodne. Redosled:

1. Konekcija na MongoDB
2. Učitavanje CSV → 6 normalizovanih kolekcija (~15 min)
3. Kreiranje indeksa
4. Pokretanje 10 base upita (~2.5h ukupno)
5. Migracija u embedded šemu (~20 min)
6. Pokretanje 10 optimizovanih upita (~3 min ukupno)
7. Uporedna analiza i grafikoni
8. Metabase vizualizacija (zahteva Docker)

### Metabase

```bash
docker run -d --name metabase --network host metabase/metabase:latest
```

Pristup: http://localhost:3000

## Struktura projekta

```
├── notebooks/
│   └── Chicago311Analysis.ipynb   # Glavni notebook
├── src/
│   ├── connection.py              # MongoDB konekcija
│   ├── load_data.py               # Učitavanje CSV-ova u MongoDB
│   ├── base_queries.py            # 10 upita za normalizovanu šemu
│   ├── optimized_queries.py       # 10 upita za embedded šemu
│   ├── optimize_schema.py         # Migracija + kreiranje indeksa
│   └── query_executor.py          # Izvršavanje i merenje vremena
├── plan_projekta.md               # Plan i opis upita
├── requirements.txt
└── data/raw/                      # CSV fajlovi (nije u repo-u)
```

## Upiti

Svaki član tima je napisao 5 upita iz perspektive određene uloge:

**Marko Zelić — Menadžer gradske infrastrukture (Q1–Q5)**

| # | Upit | Ključna tehnika |
|---|------|-----------------|
| Q1 | Infrastrukturna korelacija po ward-u | `$group` (dvostepeni), `$match` |
| Q2 | Zanemarene community areas (>1.5x prosek) | `$facet`, `$expr` |
| Q3 | Sezonski obrasci (rupe vs grafiti) | `$month`, `$group`, sezonski indeks |
| Q4 | Problematični blokovi (5+ tipova žalbi) | `$addToSet`, `$size` |
| Q5 | Efikasnost po policijskom distriktu | `$switch` (CASE WHEN) |

**Uroš Ogrizović — Analitičar javnog zdravlja i bezbednosti (Q6–Q10)**

| # | Upit | Ključna tehnika |
|---|------|-----------------|
| Q6 | Glodari po community area | `$lookup`, `$group`, `$ifNull` |
| Q7 | Napuštena vozila (30+ dana) | `$lookup`, `$match`, `$group` |
| Q8 | Sezonski obrazac glodara | Dupli `$group`, `$avg`/`$min`/`$max` |
| Q9 | Neresolvane sanitarne žalbe | `$group`, `$cond`, `$addFields` |
| Q10 | Opasne zgrade po community area | `$group`, 3× `$cond` |

## Rezultati optimizacije

Prosečno ubrzanje: **97.4%**

| Upit | Base (s) | Optimized (s) | Ubrzanje |
|------|----------|---------------|----------|
| Q1   | 795      | 6.1           | 130x     |
| Q2   | 769      | 18.3          | 42x      |
| Q3   | 746      | 6.8           | 110x     |
| Q4   | 426      | 62.3          | 7x       |
| Q5   | 1830     | 22.5          | 81x      |
| Q6   | 732      | 4.5           | 163x     |
| Q7   | 1263     | 14.5          | 87x      |
| Q8   | 653      | 3.8           | 172x     |
| Q9   | 922      | 26.9          | 34x      |
| Q10  | 1555     | 16.3          | 95x      |
