from mrjob.job import MRJob
from mrjob.step import MRStep

class AverageRatingPerActor(MRJob):

    def configure_args(self):
        super(AverageRatingPerActor, self).configure_args()
        self.add_file_arg('--ratings-file')

    def mapper_init(self):
        self.ratings = {}
        with open(self.options.ratings_file, 'r') as f:
            for line in f:
                if line.startswith('tconst'):  # Skip header
                    continue
                tokens = line.strip().split('\t')
                tconst = tokens[0]
                rating = tokens[1]
                self.ratings[tconst] = rating

    def mapper(self, _, line):
        tokens = line.split('\t')
        tconst = tokens[0]
        nconst = tokens[2]

        # skip first line with header
        if tconst == 'tconst':
            return

        if tconst in self.ratings:
            yield nconst, float(self.ratings[tconst])

    def reducer(self, key, values):
        ratings = list(values)
        yield key, sum(ratings) / len(ratings)

    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   reducer=self.reducer)
        ]

if __name__ == '__main__':
    AverageRatingPerActor.run()
