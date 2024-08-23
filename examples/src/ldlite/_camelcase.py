def _decode_triple(c1, c2, c3):
    """Decodes a sliding window of character triples.

    Examines a sequence of three characters c1, c2, and c3; decodes c2; and
    returns the decoded output.
    """
    b = ''
    c1u = c1.isupper()
    c2u = c2.isupper()
    c3u = c3.isupper()
    write = chr(0)
    write_break = False
    # First check triples that include zeros
    if c2 == chr(0):
        return b
    elif c1 == chr(0) and c2 != chr(0):
        write = c2.lower()
    elif c1 != chr(0) and c2 != chr(0) and c3 == chr(0):
        if not c1u and c2u:
            write_break = True
            write = c2.lower()
        elif c1u and c2u:
            write = c2.lower()
        elif not c1u and not c2u:
            write = c2
        elif c1u and not c2u:
            write = c2
    # Check triples having no zeros
    elif not c1u and not c2u and not c3u:
        write = c2
    elif c1u and not c2u and not c3u:
        write = c2
    elif not c1u and not c2u and c3u:
        write = c2
    elif c1u and not c2u and c3u:
        write = c2
    elif not c1u and c2u and not c3u:
        write_break = True
        write = c2.lower()
    elif c1u and c2u and not c3u:
        write_break = True
        write = c2.lower()
    elif not c1u and c2u and c3u:
        write_break = True
        write = c2.lower()
    elif c1u and c2u and c3u:
        write = c2.lower()
    else:
        # All cases should have been checked by this point
        raise ValueError('unexpected state: ("' + c1 + '", "' + c2 + '", "' + c3 + '")')
    # Write decoded characters
    if write_break:
        b += '_'
    b += write
    return b


def _decode_camel_case(s):
    """Parses camel case string into lowercase words separated by underscores.

    A sequence of uppercase letters is interpreted as a word, except that the
    last uppercase letter of a sequence is considered the start of a new word
    if it is followed by a lowercase letter.
    """
    b = ''
    # c1, c2, and c3 are a sliding window of character triples
    c1 = chr(0)
    _ = c1
    c2 = chr(0)
    c3 = chr(0)
    for c in s:
        c1 = c2
        c2 = c3
        c3 = c
        b += _decode_triple(c1, c2, c3)
    # Last character
    c1 = c2
    c2 = c3
    c3 = chr(0)
    b += _decode_triple(c1, c2, c3)
    return b
