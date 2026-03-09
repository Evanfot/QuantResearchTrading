def dict_diff(d1, d2):
    return {
        "added": d2.keys() - d1.keys(),
        "removed": d1.keys() - d2.keys(),
        "changed": {
            k: (d1[k], d2[k])
            for k in d1.keys() & d2.keys()
            if d1[k] != d2[k]
        }
    }
