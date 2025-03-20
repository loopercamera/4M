# ✅ Bibliotheken installieren (nur für colab)
# !pip install sentence-transformers -q
# !pip install scikit-learn -q

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# ✅ 1. Lade das Modell (automatisch heruntergeladen, wenn es noch nicht lokal ist)
model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')

# ✅ 2. Mobilitätswörterpool definieren
mobility_keywords = [
    "Mobilität", "Verkehr", "Transport", "Fahrt", "Auto", "Fahrrad", "Öffentlicher Nahverkehr",
    "Pendeln", "Reisen", "Bus", "Bahn", "Verkehrsmittel", "Fahrzeug", "Mobilitätskonzept", "E-Scooter",
    "Flugzeug", "Taxi", "Schiff", "Mobilitätsplattform", "Verkehrsdaten", "Verkehrsinfrastruktur"
]

# ✅ 3. Keywords in Vektoren umwandeln
keyword_embeddings = model.encode(mobility_keywords)

# ✅ 4. Funktion zur Ähnlichkeitsprüfung
def check_similarity(text, keyword_embeddings, model, threshold=0.5):
    text_embedding = model.encode([text])
    similarities = cosine_similarity(text_embedding, keyword_embeddings)
    max_similarity = np.max(similarities)
    return max_similarity >= threshold, max_similarity

# ✅ 5. Beispieltexte analysieren
texts = [
    "Dieses Dataset enthält Daten zur Anzahl der Fahrten mit öffentlichen Verkehrsmitteln.",
    "Klimadaten der letzten 10 Jahre, gemessen an verschiedenen Stationen.",
    "Informationen über die Verkaufszahlen von Online-Shops.",
    "Eine Übersicht über Pendlerströme in Großstädten.",
    "Transportnetzwerke und ihre Effizienz in urbanen Gebieten."
]

# ✅ 6. Ergebnisse anzeigen
for text in texts:
    is_mobility, similarity_score = check_similarity(text, keyword_embeddings, model)
    status = "Mobilitätsbezogen" if is_mobility else "Nicht Mobilitätsbezogen"
    print(f"Text: {text}\nErgebnis: {status} (Ähnlichkeitswert: {similarity_score:.4f})\n")
