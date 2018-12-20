#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

typedef struct _Header {
    uint64_t m_top_ref[2]; // 2 * 8 bytes
    // Info-block 8-bytes
    uint8_t m_mnemonic[4];    // "T-DB"
    uint8_t m_file_format[2]; // See `library_file_format`
    uint8_t m_reserved;
    // bit 0 of m_flags is used to select between the two top refs.
    uint8_t m_flags;
} Header;

char to_print(unsigned char ch)
{
    return (ch >= 0x20 && ch <= 0x7e) ? (char)ch : '.';
}

void dump_buffer(unsigned char* buffer, unsigned long addr, size_t sz)
{
    char printable[20];
    size_t index = 0;
    unsigned char* ptr = buffer;
    unsigned char* end = buffer + sz;
    while (ptr < end) {
        size_t len = (end - ptr);
        if (len > 16)
            len = 16;
        printf("%08zx  ", addr + (ptr - buffer));
        char* trans = printable;
        *trans++ = '|';
        for (size_t i = 0; i < 16; i++) {
            if (i == 8)
                printf(" ");
            if (i < len) {
                printf("%02x ", *ptr);
                *trans++ = to_print(*ptr++);
            }
            else {
                printf("   ");
            }
        }
        *trans++ = '|';
        *trans = '\0';
        printf(" %s\n", printable);
    }
}

void dump(FILE* fp, long offset, size_t sz)
{
    if (sz) {
        fseek(fp, offset, SEEK_SET);
        unsigned char* buffer = malloc(sz);
        size_t actual = fread(buffer, 1, sz, fp);
        dump_buffer(buffer, offset, actual);
        free(buffer);
    }
}

size_t dump_header(FILE* fp, long offset)
{
    unsigned char header[8];
    fseek(fp, offset, SEEK_SET);
    size_t actual = fread(header, 1, 8, fp);
    if (strncmp(header, "AAAA", 4) != 0) {
        printf("Ref '0x%lx' does not point to an array\n", (unsigned long)(offset));
        dump(fp, offset, 64);
        return 0;
    }
    size_t size = (header[5] << 16) + (header[6] << 8) + header[7];

    unsigned flags = header[4];
    unsigned wtype = (flags & 0x18) >> 3;
    unsigned width = (1 << (flags & 0x07)) >> 1;

    size_t num_bytes = 0;
    const char* type = "";
    switch (wtype) {
        case 0: {
            assert(size < 0x1000000);
            size_t num_bits = size * width;
            num_bytes = (num_bits + 7) >> 3;
            type = "bits";
            break;
        }
        case 1: {
            num_bytes = size * width;
            type = "bytes";
            break;
        }
        case 2:
            num_bytes = size;
            break;
    }

    printf("Ref: %lx, Size: %zd, width: %d %s \n", offset, size, width, type);

    return num_bytes;
}

void dump_file_header(FILE* fp)
{
    Header header;
    fseek(fp, 0, SEEK_SET);
    size_t actual = fread(&header, sizeof(Header), 1, fp);
    dump_buffer((unsigned char*)&header, 0, 24);
    size_t sz = dump_header(fp, header.m_top_ref[0]);
    dump(fp, header.m_top_ref[0] + 8, sz);
    sz = dump_header(fp, header.m_top_ref[1]);
    dump(fp, header.m_top_ref[1] + 8, sz);
}

void usage()
{
    printf("Usage: realm-dump <file> [<ref>]\n");
    exit(1);
}

int main(int argc, const char* argv[])
{
    if (argc < 2 || argc > 3) {
        usage();
    }

    FILE* fp = fopen(argv[1], "r");
    if (!fp) {
        printf("File '%s' not found\n", argv[1]);
        exit(1);
    }

    printf("File: '%s'\n", argv[1]);
    long ref = 0;
    if (argc == 3) {
        char* end;
        ref = strtol(argv[2], &end, 0);
        if (*end != '\0') {
            printf("'%s' is not a number\n", argv[2]);
            exit(1);
        }
    }
    if (ref) {
        size_t sz = dump_header(fp, ref);
        dump(fp, ref + 8, sz);
    }
    else {
        dump_file_header(fp);
    }

    printf("\n");

    fclose(fp);

    return 0;
}