from mrjob.job import MRJob
from mrjob.step import MRStep

class SingleSourceShortestPath(MRJob):
    # SSSP with MapReduce, using parallel BFS.

    def configure_args(self):
        super(SingleSourceShortestPath, self).configure_args()
        self.add_passthru_arg('--source', default='nm0000102', help='source node')
        self.add_passthru_arg('--depth', default=6, help='degree of separation')


    # ------------------------------------------------------------------------#
    #   Preprocessing: mapper1, reducer1, mapper2, reducer2
    # ------------------------------------------------------------------------#

    def mapper1(self, _, line):
        tokens = line.split('\t')
        tconst = tokens[0]
        nconst = tokens[2]

        # skip first line with header
        if tconst == 'tconst':
            return

        yield tconst, nconst

    def reducer1(self, key, values):
        yield key, list(values)

    # after mapper1 and reducer1, we have all actors per film:
    # "tconst"  [nconst list]
    # e.g. "tt0000002"	["nm0000002","nm0000003"]

    def mapper2(self, key, values):
        for i in range(len(values)):
            for j in range(i+1, len(values)):
                yield values[i], values[j]
                yield values[j], values[i]

    def reducer2(self, key, values):
        if key == self.options.source:
            yield key, (0, list(values))
        else:
            yield key, (10000, list(values)) # 10000 is infinity

    # after mapper2 and reducer2, we have adjacency lists per actor,
    # with distance to source node initialized to 10000 (infinity):
    # "nconst"  [distance, [adjacency list]]
    # e.g. "nm0000002"	[10000, ["nm0000003","nm0000001"]]
    # except for the source node, which has distance 0 to itself:
    # e.g. "nm0000102"	[0, ["nm0000001", "nm0000002"]]


    # ------------------------------------------------------------------------#
    #   Parallel BFS: mapper3, reducer3
    # ------------------------------------------------------------------------#

    def mapper3(self, key, values):
        yield key, values[1]  # pass graph structure
        yield key, values[0]  # pass known distances

        for nconst in values[1]:
            yield nconst, values[0] + 1  # emit distances to reachable nodes

    def reducer3(self, key, values):
        min_distance = 10000
        adjacency_list = []

        for value in values:
            if isinstance(value, int):
                if value < min_distance:  # look for shorter distance
                    min_distance = value
            else:
                adjacency_list = value  # recover graph structure

        yield key, (min_distance, adjacency_list)

    # At each iteration of mapper3 and reducer3, we explore one more "level" of the graph. 
    # Think of the parallel BFS as a frontier of nodes that expands in all directions, one level at a time.
    # We also keep track of the minimum distance to the source node. 
    # Once a node is reached for the first time, since all paths have a cost = 1 and all nodes are expanded in parallel by only 1 level at each iteration, it is guaranteed to be the shortest path.


    # ------------------------------------------------------------------------#
    #   Postprocessing: mapper4
    # ------------------------------------------------------------------------#

    # mapper4 output nconst, distance only, removing adjacency lists for clarity
    def mapper4(self, key, values):
        yield key, values[0]


    def steps(self):
        steps = []

        # Preprocessing
        steps.append(MRStep(mapper=self.mapper1, reducer=self.reducer1))
        steps.append(MRStep(mapper=self.mapper2, reducer=self.reducer2))

        # Parallel BFS
        for i in range(int(self.options.depth)):
            steps.append(MRStep(mapper=self.mapper3, reducer=self.reducer3))

        # Postprocessing
        steps.append(MRStep(mapper=self.mapper4))

        return steps


if __name__ == '__main__':
    SingleSourceShortestPath.run()
