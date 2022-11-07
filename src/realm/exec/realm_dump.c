#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <ctype.h>
#include <assert.h>

#ifdef _WIN64
#define do_seek _fseeki64
#else
#define do_seek fseek
#endif

typedef struct _FileHeader {
    uint64_t m_top_ref[2]; // 2 * 8 bytes
    // Info-block 8-bytes
    uint8_t m_mnemonic[4];    // "T-DB"
    uint8_t m_file_format[2]; // See `library_file_format`
    uint8_t m_reserved;
    // bit 0 of m_flags is used to select between the two top refs.
    uint8_t m_flags;
} FileHeader;

typedef struct _NodeHeader {
    unsigned wtype;
    unsigned width;
    int is_inner;
    int has_refs;
    int context;
    size_t size;
    char* type;
    size_t num_bytes;
} NodeHeader;

char to_print(unsigned char ch)
{
    return (ch >= 0x20 && ch <= 0x7e) ? (char)ch : '.';
}

void dump_buffer(unsigned char* buffer, uint64_t addr, size_t sz)
{
    char printable[20];
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

int get_header(NodeHeader* node_header, FILE* fp, int64_t offset)
{
    memset(node_header, 0, sizeof(NodeHeader));
    unsigned char header[8];
    do_seek(fp, offset, SEEK_SET);
    fread(header, 1, 8, fp);
    if (strncmp((const char*)header, "AAAA", 4) != 0) {
        printf("Ref '0x%zx' does not point to an array\n", offset);
        dump(fp, offset, 64);
        return 0;
    }
    /* dump_buffer(header, offset, 8); */

    node_header->size = (header[5] << 16) + (header[6] << 8) + header[7];

    unsigned flags = header[4];
    node_header->wtype = (flags & 0x18) >> 3;
    node_header->width = (1 << (flags & 0x07)) >> 1;
    node_header->is_inner = (flags & 0x80) ? 1 : 0;
    node_header->has_refs = (flags & 0x40) ? 1 : 0;
    node_header->context = (flags & 0x20) ? 1 : 0;

    node_header->type = "";
    switch (node_header->wtype) {
        case 0: {
            assert(node_header->size < 0x1000000);
            size_t num_bits = node_header->size * node_header->width;
            node_header->num_bytes = (num_bits + 7) >> 3;
            node_header->type = "bits";
            break;
        }
        case 1: {
            node_header->num_bytes = node_header->size * node_header->width;
            node_header->type = "bytes";
            break;
        }
        case 2:
            node_header->num_bytes = node_header->size;
            break;
    }
    return 1;
}

size_t dump_header(FILE* fp, int64_t offset)
{
    NodeHeader header;
    if (get_header(&header, fp, offset)) {
        if (header.is_inner && header.has_refs) {
            printf("Ref: 0x%zx, Size: %zd, width: %d %s Inner B+tree node\n", offset, header.size, header.width,
                   header.type);
        }
        else {
            printf("Ref: 0x%zx, Size: %zd, width: %d %s, hasRefs: %d, flag: %d\n", offset, header.size, header.width,
                   header.type, header.has_refs, header.context);
        }
    }

    return header.num_bytes;
}

void dump_file_header(FILE* fp)
{
    FileHeader header;
    do_seek(fp, 0, SEEK_SET);
    fread(&header, sizeof(FileHeader), 1, fp);
    dump_buffer((unsigned char*)&header, 0, 24);
    size_t sz = dump_header(fp, header.m_top_ref[0]);
    dump(fp, header.m_top_ref[0] + 8, sz);
    sz = dump_header(fp, header.m_top_ref[1]);
    dump(fp, header.m_top_ref[1] + 8, sz);
}

int64_t get_top_ref(FILE* fp)
{
    FileHeader header;
    do_seek(fp, 0, SEEK_SET);
    fread(&header, sizeof(FileHeader), 1, fp);
    return header.m_top_ref[header.m_flags];
}

int search_ref(FILE* fp, int64_t ref, int64_t target, size_t level, int* stack)
{
    NodeHeader header;
    get_header(&header, fp, ref);
    if (header.has_refs) {
        assert(header.width >= 8);
        size_t byte_size = header.width / 8;
        char buffer[byte_size * header.size];
        do_seek(fp, ref + 8, SEEK_SET);
        fread(buffer, byte_size * header.size, 1, fp);
        for (size_t i = 0; i < header.size; i++) {
            stack[level] = i;
            int64_t subref = 1;
            switch (byte_size) {
                case 1:
                    subref = buffer[i];
                    break;
                case 2:
                    subref = ((int16_t*)buffer)[i];
                    break;
                case 4:
                    subref = ((int32_t*)buffer)[i];
                    break;
                case 8:
                    subref = ((int64_t*)buffer)[i];
                    break;
            }
            if (subref && (subref & 1) == 0) {
                if (subref == target) {
                    printf("Ref '0x%zx' found at [", target);
                    for (size_t j = 0; j < level + 1; j++) {
                        if (j == 0)
                            printf("%d", stack[j]);
                        else
                            printf(",%d", stack[j]);
                    }
                    printf("]\n");
                    return 1;
                }
                if (search_ref(fp, subref, target, level + 1, stack))
                    return 1;
            }
        }
    }
    return 0;
}

void dump_index(FILE* fp, int64_t ref, const char* arr)
{
    char* p = (char*)arr;
    unsigned idx = strtoll(arr, &p, 0);
    NodeHeader header;
    get_header(&header, fp, ref);
    if (!header.has_refs) {
        printf("Ref '0x%zx' does not point to an array with refs\n", ref);
        dump_header(fp, ref);
        exit(1);
    }
    if (idx >= header.size) {
        printf("Index '%d' is out of bounds (size = %d)\n", idx, (int)header.size);
        dump_header(fp, ref);
        exit(1);
    }

    int64_t subref = 0;
    assert(header.width >= 8);
    size_t byte_size = header.width / 8;
    int64_t offset = ref + 8 + byte_size * idx;
    do_seek(fp, offset, SEEK_SET);
    fread(&subref, byte_size, 1, fp);

    if (subref & 1) {
        printf("Value '%ld' is not a subref\n", subref);
        exit(1);
    }
    while (isspace(*p))
        p++;
    if (*p == ',') {
        p++;
        dump_index(fp, subref, p);
    }
    else {
        printf("looking up index %d at 0x%zx = 0x%zx\n", idx, (size_t)offset, subref);
        size_t sz = dump_header(fp, subref);
        dump(fp, subref + 8, sz);
    }
}

void usage()
{
    printf("Usage: realm-dump <file> [?][<ref>] [<array>]\n");
    exit(1);
}

int main(int argc, const char* argv[])
{
    if (argc < 2) {
        usage();
    }

    FILE* fp = fopen(argv[1], "rb");
    if (!fp) {
        printf("File '%s' not found\n", argv[1]);
        exit(1);
    }

    printf("File: '%s'\n", argv[1]);
    int64_t ref = 0;
    int64_t find_ref = 0;
    const char* array_str = NULL;
    for (size_t arg = 2; arg < argc; arg++) {
        if (*argv[arg] == '[') {
            array_str = argv[arg] + 1;
        }
        else if (*argv[arg] == '?') {
            char* end;
            find_ref = strtoll(argv[arg] + 1, &end, 0);
            if (*end != '\0') {
                printf("'%s' is not a number\n", argv[2]);
                exit(1);
            }
        }
        else {
            char* end;
            ref = strtoll(argv[arg], &end, 0);
            if (*end != '\0') {
                printf("'%s' is not a number\n", argv[2]);
                exit(1);
            }
        }
    }

    if (array_str) {
        if (!ref)
            ref = get_top_ref(fp);
        dump_index(fp, ref, array_str);
    }
    else if (ref) {
        size_t sz = dump_header(fp, ref);
        dump(fp, ref + 8, sz);
    }
    else if (find_ref) {
        int stack[128];
        ref = get_top_ref(fp);
        search_ref(fp, ref, find_ref, 0, stack);
    }
    else {
        dump_file_header(fp);
    }

    fclose(fp);

    return 0;
}
