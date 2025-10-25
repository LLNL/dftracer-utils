#ifndef DFTRACER_UTILS_COMPONENTS_TEXT_TEXT_HASHER_H
#define DFTRACER_UTILS_COMPONENTS_TEXT_TEXT_HASHER_H

#include <dftracer/utils/components/text/shared.h>
#include <dftracer/utils/core/utilities/utility.h>
#include <xxhash.h>

#include <string>

namespace dftracer::utils::components::text {

/**
 * @brief Hash algorithm to use for text hashing.
 */
enum class HashAlgorithm {
    XXH64,    // xxHash 64-bit (fast, high quality)
    XXH3_64,  // xxHash3 64-bit (fastest, recommended)
    STD       // std::hash (platform-dependent)
};

/**
 * @brief Utility that computes a hash value for text content.
 *
 * This utility takes text (or a line) and computes a hash value using
 * the selected algorithm. Useful for deduplication, indexing, and checksums.
 *
 * Features:
 * - Multiple hash algorithms (xxHash3, xxHash, std::hash)
 * - Fast and high-quality hashing
 * - Can be tagged with Cacheable, Retryable, Monitored behaviors
 *
 * Usage:
 * @code
 * auto hasher = std::make_shared<TextHasher>();
 * hasher->set_algorithm(HashAlgorithm::XXH3_64);  // Fastest
 *
 * Text input("Hello, World!");
 * Hash hash = hasher->process(input);
 *
 * std::cout << "Hash: " << hash.value << "\n";
 * @endcode
 *
 * Hashing lines:
 * @code
 * auto line_hasher = std::make_shared<LineHasher>();
 *
 * Line line{"Event data here", 42};
 * Hash hash = line_hasher->process(line);
 * @endcode
 */
class TextHasher : public utilities::Utility<Text, Hash> {
   private:
    HashAlgorithm algorithm_ = HashAlgorithm::XXH3_64;  // Default: fastest

   public:
    TextHasher() = default;
    ~TextHasher() override = default;

    /**
     * @brief Set the hash algorithm to use.
     */
    void set_algorithm(HashAlgorithm algo) { algorithm_ = algo; }

    HashAlgorithm get_algorithm() const { return algorithm_; }

    /**
     * @brief Compute hash of text content.
     *
     * @param input Text to hash
     * @return Hash value
     */
    Hash process(const Text& input) override {
        return Hash{compute_hash(input.content)};
    }

   private:
    std::size_t compute_hash(const std::string& data) const {
        switch (algorithm_) {
            case HashAlgorithm::XXH64:
                return static_cast<std::size_t>(
                    XXH64(data.data(), data.size(), 0));

            case HashAlgorithm::XXH3_64:
                return static_cast<std::size_t>(
                    XXH3_64bits(data.data(), data.size()));

            case HashAlgorithm::STD:
                return std::hash<std::string>{}(data);

            default:
                return std::hash<std::string>{}(data);
        }
    }
};

/**
 * @brief Utility that computes a hash value for a single line.
 *
 * Similar to TextHasher but operates on Line instead of Text.
 * Hashes the line content only (ignores line_number).
 */
class LineHasher : public utilities::Utility<Line, Hash> {
   private:
    HashAlgorithm algorithm_ = HashAlgorithm::XXH3_64;

   public:
    LineHasher() = default;
    ~LineHasher() override = default;

    void set_algorithm(HashAlgorithm algo) { algorithm_ = algo; }

    HashAlgorithm get_algorithm() const { return algorithm_; }

    /**
     * @brief Compute hash of line content.
     *
     * @param input Line to hash
     * @return Hash value
     */
    Hash process(const Line& input) override {
        return Hash{compute_hash(input.content)};
    }

   private:
    std::size_t compute_hash(const std::string& data) const {
        switch (algorithm_) {
            case HashAlgorithm::XXH64:
                return static_cast<std::size_t>(
                    XXH64(data.data(), data.size(), 0));

            case HashAlgorithm::XXH3_64:
                return static_cast<std::size_t>(
                    XXH3_64bits(data.data(), data.size()));

            case HashAlgorithm::STD:
                return std::hash<std::string>{}(data);

            default:
                return std::hash<std::string>{}(data);
        }
    }
};

}  // namespace dftracer::utils::components::text

#endif  // DFTRACER_UTILS_COMPONENTS_TEXT_TEXT_HASHER_H
