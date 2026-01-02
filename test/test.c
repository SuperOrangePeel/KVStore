#include <stdio.h>

void func(int *i, int length) {
    *i = length / 2;
}

int main() {
    int i = 0;
    int length = 10;
    func(&i, length);
    printf("i:%d\n", i);

    return 0;
}