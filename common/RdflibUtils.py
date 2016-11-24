__author__ = 'wnewell'
"""
Static methods for RDFLib Graphs
Example methods:
- get_subject_from_label(g, label)
    inputs:
        g: RDFLib CojugativeGraph
        label: label of subject
    output: subject

- get_subject_properties(g, s):
    objectList = list(g.predicate_objects(s))
    ...

- get_alternative_terms(g, s):
    predicate = rdflib.term.URIRef(u'http://www.ebi.ac.uk/efo/alternative_term')
    - alternative_term ->



"""


def main():
    print "main()"
    test()


def test():
    print "test()"


if __name__ == '__main__':
    main()
    
    
