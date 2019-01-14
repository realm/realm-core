#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

#ifdef _WIN64
#define do_seek _fseeki64
#else
#define do_seek fseek
#endif

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

void dump_buffer(unsigned char* buffer, uint64_t addr, size_t sz)
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

void dump(FILE* fp, int64_t offset, size_t sz)
{
    if (sz) {
        do_seek(fp, offset, SEEK_SET);
        unsigned char* buffer = malloc(sz);
        size_t actual = fread(buffer, 1, sz, fp);
        if (actual != sz) {
            if (feof(fp)) {
                printf("*** Unexpected EOF\n");
            }
            else {
                int err = ferror(fp);
                printf("*** Read error code: %d\n", err);
            }
        }
        dump_buffer(buffer, offset, actual);
        free(buffer);
    }
}

size_t dump_header(FILE* fp, int64_t offset)
{
    unsigned char header[8];
    do_seek(fp, offset, SEEK_SET);
    size_t actual = fread(header, 1, 8, fp);
    if (strncmp(header, "AAAA", 4) != 0) {
        printf("Ref '0x%zx' does not point to an array\n", offset);
        dump(fp, offset, 64);
        return 0;
    }
    /* dump_buffer(header, offset, 8); */

    size_t size = (header[5] << 16) + (header[6] << 8) + header[7];

    unsigned flags = header[4];
    unsigned wtype = (flags & 0x18) >> 3;
    unsigned width = (1 << (flags & 0x07)) >> 1;
    int is_inner = (flags & 0x80) ? 1 : 0;
    int has_refs = (flags & 0x40) ? 1 : 0;
    int context = (flags & 0x20) ? 1 : 0;

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

    if (is_inner && has_refs) {
        printf("Ref: %zx, Size: %zd, width: %d %s Inner B+tree node\n", offset, size, width, type);
    }
    else {
        printf("Ref: %zx, Size: %zd, width: %d %s, hasRefs: %d\n", offset, size, width, type, has_refs);
    }

    return num_bytes;
}

void dump_file_header(FILE* fp)
{
    Header header;
    do_seek(fp, 0, SEEK_SET);
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

    FILE* fp = fopen(argv[1], "rb");
    if (!fp) {
        printf("File '%s' not found\n", argv[1]);
        exit(1);
    }

    printf("File: '%s'\n", argv[1]);
    int64_t ref = 0;
    if (argc == 3) {
        char* end;
        ref = strtoll(argv[2], &end, 0);
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