
from mrjob.job import MRJob 
from mrjob.step import MRStep

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
