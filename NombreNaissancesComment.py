# Programme : NombreNaissances
# Calcul via MapReduce du nombre de naissances par prenom
# 
# Auteur : Ajiriti Services
# Version : 1.0
# Date : Septembre 2020
#
from mrjob.job import MRJob # Importation de la librairie d'ecriture de jobs MapReduce en Python
from mrjob.step import MRStep # Etapes des jobs MapReduce

# La Classe NombreNaissances définie 3 methodes
# steps - Represente les etapes du job MapReduce
# mapper_naissances_prenom - Fonction pour le job Map
# reducer_compte_naissances - Fonction pour lejob Reducer
class NombreNaissances(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_naissances_prenom,
                   reducer=self.reducer_compte_naissances)
        ]

    def mapper_naissances_prenom(self, _, line):
        (sexe, preusuel, annais, nombre) = line.split(';')
        yield preusuel, int(nombre)

    def reducer_compte_naissances(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    NombreNaissances.run()
