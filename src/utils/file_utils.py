# src/utils/file_utils.py

def compter_parquet(path, dbutils):
    """
    Compte le nombre de fichiers parquet et leur taille totale
    """
    fichiers = dbutils.fs.ls(path)
    nb = 0
    taille = 0

    for f in fichiers:
        if f.isDir():
            n, t = compter_parquet(f.path, dbutils)
            nb += n
            taille += t
        elif f.name.endswith(".parquet"):
            nb += 1
            taille += f.size

    return nb, taille
