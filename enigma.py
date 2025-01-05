def encrypt(a):
    A = "nymphsblitzquIckvexdwarfjog."
    b,n,i,z = A, 28, 0, 0
    c = ""
    while (i < len(a)):
        x = A.index(a[i])
        y = A.index(b[i % len(b)])
        z = (x + y) % n
        c = c + A[z]
        if (x + 1 == n):
            b = c
        i = i+1
    return c

def decrypt(c):
    A = "nymphsblitzquIckvexdwarfjog."
    b, n, i = A, 28, 0
    a = ""
    while i < len(c):
        z = A.index(c[i])
        y = A.index(b[i % len(b)])
        # need to get the correct index of the character in A
        x = (z - y)
        a = a + A[x]
        if A[x] == ".":
            b = c[0:i+1]
        i = i + 1
    return a


decrypt(encrypt("countries"))

print(decrypt("tsdmueyuvrxIedqqfmdqweIyaaxtiyzrujqezxqdawgotw"))