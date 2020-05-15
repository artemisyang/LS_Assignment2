from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):

    def mapper_get_words(self, _, row):
        '''
        If a review's star rating is 5, yield all of the words in the review
        '''
        data = row.split('\t')
        if data[7] == '5':
            for word in WORD_RE.findall(data[13]):
                yield (word.lower(), 1)

    def combiner_count_words(self, word, counts):
        '''
        Sum all of the words available so far
        '''
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        '''
        Arrive at a total count for each word in the 5 star reviews
        '''
        yield None, (sum(counts), word)

    # discard the key; it is just None
    def reducer_find_max_word(self, _, word_count_pairs):
        '''
        Yield the word that occurs the most in the 5 star reviews
        '''
        yield max(word_count_pairs)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]

if __name__ == '__main__':
    MRMostUsedWord.run()
