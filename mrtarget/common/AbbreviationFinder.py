#!/usr/local/bin/python
# -*- coding: UTF-8 -*-

'''Link abbreviations to their full names | Optimised to find the longest definition


Adapted and optimised from source available here: source: http://www.cnts.ua.ac.be/~vincent/scripts/abbreviations.py
source made available by: Vincent Van Asch
original source version: 1.2.1
original alghoritm in:

A Simple Algorithm for Identifying Abbreviations Definitions in Biomedical Text
A. Schwartz and M. Hearst
Biocomputing, 2003, pp 451-462.

'''
import logging
import re

from textblob import TextBlob


class Candidate(unicode):
    def __new__(cls, start, stop, str):
        return unicode.__new__(cls, str)

    def __init__(self, start, stop, str):
        self._start = start
        self._stop = stop

    def __getslice__(self, i, j):
        start = self.start + i
        stop = self.start + j
        str = unicode.__getslice__(self, i, j)
        return Candidate(start, stop, str)

    @property
    def start(self):
        '''The start index'''
        return self._start

    @property
    def stop(self):
        '''The stop index'''
        return self._stop





class AbbreviationsParser(object):
    def __init__(self, verbose = False):
        self.encoding = 'UTF8'
        self.verbose = verbose
        self.logger = logging.getLogger(__name__)

    def digest(self, textblob):
        if isinstance(textblob, (str, unicode)):
           textblob = TextBlob(textblob)
        return list(self._digest_iterator(textblob))

    def digest_as_dict(self, textblob):
        digested = self.digest(textblob)
        d = {}
        for i in digested:
            if i['short'] not in d:
                d[i['short']]=i['long']
        return d

    def _digest_iterator(self, textblob):
        omit = 0
        written = 0
        for i, sentence in enumerate(textblob.sentences):
            sentence = sentence.raw
            # print sentence
            try:
                for candidate in self.getcandidates(sentence):
                    try:
                        definition = self.getdefinition(candidate, sentence)
                    except ValueError as e:
                        if self.verbose:
                            self.logger.debug(str((i, 'Omitting candidate', candidate.encode(self.encoding), 'Reason:',
                                               e.args[0].encode(self.encoding))))
                        omit += 1
                    else:
                        try:
                            definition = self.definitionselection(definition, candidate)
                        except IndexError:
                            if self.verbose:
                                self.logger.debug(str((i, 'Omitting candidate', definition.encode(
                                self.encoding), '||', candidate.encode(self.encoding))))
                            omit += 1
                        except ValueError as e:
                            if self.verbose:
                                self.logger.debug(str((i, 'Omitting candidate', definition.encode(
                                self.encoding), '||', candidate.encode(self.encoding), 'Reason:',
                                                   e.args[0].encode(self.encoding))))
                            omit += 1
                        else:

                            cline = '%d %d %d %s' % (i, candidate.start, candidate.stop, candidate)
                            dline = '%d %d %d %s' % (i, definition.start, definition.stop, definition)

                            yield dict(short=candidate.encode(self.encoding),
                                        long=definition.encode(self.encoding))
                            # print cline.encode(self.encoding)
                            # print dline.encode(self.encoding)
                            # print

                            written += 1
            except ValueError as e:
                if self.verbose:
                    self.logger.debug(str(('Reason:', e.args[0].encode(self.encoding))))

    def getcandidates(self, sentence):
        '''Yields Candidates'''
        delimiters = {'(': ('(', ')'),
                      '[': ('[', ']'),
                      '{': ('{', '}'),
                      '<': ('<', '>'), }
        for delimiter in delimiters:
            if delimiter in sentence:
                del_start, del_end = delimiters[delimiter]
                # Check some things first
                if sentence.count(del_start) != sentence.count(del_end):
                    raise ValueError('Unbalanced parentheses: %s' % sentence)

                if sentence.find(del_start) > sentence.find(del_end):
                    raise ValueError('First parentheses is right: %s' % sentence)

                closeindex = -1
                while 1:
                    # Look for open parenthesis
                    openindex = sentence.find(del_start, closeindex + 1)

                    if openindex == -1:
                        break

                    # Look for closing parantheses
                    closeindex = openindex + 1
                    open = 1
                    skip = False
                    while open:
                        try:
                            char = sentence[closeindex]
                        except IndexError:
                            # We found an opening bracket but no associated closing bracket
                            # Skip the opening bracket
                            skip = True
                            break
                        if char == del_start:
                            open += 1
                        elif char == del_end:
                            open -= 1
                        closeindex += 1

                    if skip:
                        closeindex = openindex + 1
                        continue

                    # Output if conditions are met
                    start = openindex + 1
                    stop = closeindex - 1
                    str = sentence[start:stop]

                    # Take into account whitepsace that should be removed
                    start = start + len(str) - len(str.lstrip())
                    stop = stop - len(str) + len(str.rstrip())
                    str = sentence[start:stop]

                    if self.conditions(str):
                        yield Candidate(start, stop, str)

    def getdefinition(self, candidate, sentence):
        '''Takes a candidate and a sentence and returns the definition candidate.

           The definintion candidate is the set of tokens (in front of the candidate)
           that starts with a token starting with the first character of the candidate'''
        # Take the tokens in front of the candidate
        tokens = sentence[:candidate.start - 2].lower().split()

        # the char that we are looking for
        key = candidate[0].lower()

        # Count the number of tokens that start with the same character as the candidate
        firstchars = [t[0] for t in tokens]

        definitionfreq = firstchars.count(key)
        candidatefreq = candidate.lower().count(key)

        # Look for the list of tokens in front of candidate that
        # have a sufficient number of tokens starting with key
        if candidatefreq <= definitionfreq:
            # we should at least have a good number of starts
            count = 0
            start = 0
            startindex = len(firstchars) - 1
            while count < candidatefreq:
                if abs(start) > len(firstchars):
                    raise ValueError('not found')

                start -= 1
                # Look up key in the definition
                try:
                    startindex = firstchars.index(key, len(firstchars) + start)
                except ValueError:
                    pass

                # Count the number of keys in definition
                count = firstchars[startindex:].count(key)

            # We found enough keys in the definition so return the definition as a
            # definition candidate
            start = len(' '.join(tokens[:startindex]))
            stop = candidate.start - 2
            str = sentence[start:stop]

            # Remove whitespace
            start = start + len(str) - len(str.lstrip())
            stop = stop - len(str) + len(str.rstrip())
            str = sentence[start:stop]

            return Candidate(start, stop, str)


        else:
            # print 'S', sentence
            # print >>sys.stderr, 'KEY', key
            # print >>sys.stderr, 'TOKENS', tokens
            # print >>sys.stderr, 'ABBREV', candidate
            raise ValueError('There are less keys in the tokens in front of candidate than there are in the candidate')

    def definitionselection(self, definition, abbrev,):
        '''Takes a definition candidate and an abbreviation candidate
        and returns True if the chars in the abbreviation occur in the definition

        Based on
        A simple algorithm for identifying abbreviation definitions in biomedical texts, Schwartz & Hearst'''

        def get_matches():
            '''yield a list of possible dfinitions'''
            if len(definition) < len(abbrev):
                raise ValueError('Abbreviation is longer than definition')

            if abbrev in definition.split():
                raise ValueError('Abbreviation is full word of definition')

            sindex = -1
            lindex = -1

            while 1:
                try:
                    longchar = definition[lindex].lower()
                except IndexError:
                    break

                shortchar = abbrev[sindex].lower()

                if not shortchar.isalnum():
                    sindex -= 1

                if sindex == -1 * len(abbrev):
                    if shortchar == longchar:
                        if lindex == -1 * len(definition) or not definition[lindex - 1].isalnum():
                            yield definition[lindex:len(definition)]
                        lindex -= 1
                        if lindex == -1 * len(definition):
                            break

                    else:
                        lindex -= 1

                        if lindex == -1 * (len(definition) + 1):
                            raise ValueError('definition of "%s" not found in "%s"' % (abbrev, definition))

                else:
                    if shortchar == longchar:
                        sindex -= 1
                        lindex -= 1
                    else:
                        lindex -= 1

        definitions = list(get_matches())
        if not definitions:
            raise IndexError('no matching definition found')
        definition = definitions[0]
        for i in definitions:
            if len(i) > len(definition):
                definition = i
        tokens = len(definition.split())
        length = len(abbrev)

        if tokens > min([length + 5, length * 2]):
            raise ValueError('did not meet min(|A|+5, |A|*2) constraint')

        return definition

    def conditions(self, str):
        '''Based on Schwartz&Hearst

        2 <= len(str) <= 10
        len(tokens) <= 2
        re.search('[A-Za-z]', str)
        str[0].isalnum()

        and extra:
        if it matches ([A-Za-z]\. ?){2,}
        it is a good candidate.

        '''
        # import nltk
        # if nltk.re.match('([A-Za-z]\. ?){2,}', str.lstrip()):
        #     return True
        if len(str) < 2 or len(str) > 10:
            return False
        if len(str.split()) > 2:
            return False
        if not re.search('[A-Za-z]', str):
            return False
        if not str[0].isalnum():
            return False

        return True
