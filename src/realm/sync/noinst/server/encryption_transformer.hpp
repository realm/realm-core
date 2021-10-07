#ifndef REALM_NOINST_ENCRYPTION_TRANSFORMER_HPP
#define REALM_NOINST_ENCRYPTION_TRANSFORMER_HPP

#include <array>
#include <string>

#include <realm/util/optional.hpp>

namespace realm {
namespace encryption_transformer {

struct Configuration {
    util::Optional<std::array<char, 64>> input_key;
    util::Optional<std::array<char, 64>> output_key;
    bool verbose = false;
    enum class TransformType {
        File,
        FileContaingPaths,
    } type;
    std::string target_path;
    util::Optional<size_t> jobs;
};

// returns the number of files successfully transformed
size_t encrypt_transform(const Configuration&);

} // namespace encryption_transformer
} // namespace realm

#endif // REALM_NOINST_ENCRYPTION_TRANSFORMER_HPP
