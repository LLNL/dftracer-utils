#ifndef DFTRACER_UTILS_COMPONENTS_TEXT_SHARED_H
#define DFTRACER_UTILS_COMPONENTS_TEXT_SHARED_H

#include <functional>
#include <optional>
#include <string>
#include <vector>

namespace dftracer::utils::components::text {

/**
 * @brief A single line of text.
 */
struct Line {
    std::string content;
    std::size_t line_number = 0;  // Optional line number (0 = not set)

    Line() = default;

    explicit Line(std::string str, std::size_t line_num = 0)
        : content(std::move(str)), line_number(line_num) {}

    // Equality for caching support
    bool operator==(const Line& other) const {
        return content == other.content && line_number == other.line_number;
    }

    bool operator!=(const Line& other) const { return !(*this == other); }

    bool empty() const { return content.empty(); }

    std::size_t size() const { return content.size(); }
};

/**
 * @brief Multiple lines of text.
 */
struct Lines {
    std::vector<Line> lines;

    Lines() = default;

    explicit Lines(std::vector<Line> ls) : lines(std::move(ls)) {}

    explicit Lines(const std::vector<std::string>& strings) {
        lines.reserve(strings.size());
        std::size_t line_num = 1;
        for (const auto& str : strings) {
            lines.emplace_back(str, line_num++);
        }
    }

    // Equality for caching support
    bool operator==(const Lines& other) const { return lines == other.lines; }

    bool operator!=(const Lines& other) const { return !(*this == other); }

    std::size_t size() const { return lines.size(); }

    bool empty() const { return lines.empty(); }
};

/**
 * @brief Multi-line text content.
 */
struct Text {
    std::string content;

    Text() = default;

    explicit Text(std::string str) : content(std::move(str)) {}

    // Equality for caching support
    bool operator==(const Text& other) const {
        return content == other.content;
    }

    bool operator!=(const Text& other) const { return !(*this == other); }

    bool empty() const { return content.empty(); }

    std::size_t size() const { return content.size(); }
};

/**
 * @brief A line with a predicate function for filtering.
 */
struct FilterableLine {
    Line line;
    std::function<bool(const Line&)> predicate;

    FilterableLine() = default;

    FilterableLine(Line l, std::function<bool(const Line&)> pred)
        : line(std::move(l)), predicate(std::move(pred)) {}

    // Note: Cannot easily define equality for std::function, so filtering
    // utilities should not use Cacheable tag unless they provide a custom key
};

/**
 * @brief Hash value for a string/line.
 */
struct Hash {
    std::size_t value = 0;

    Hash() = default;

    explicit Hash(std::size_t v) : value(v) {}

    // Equality for caching support
    bool operator==(const Hash& other) const { return value == other.value; }

    bool operator!=(const Hash& other) const { return !(*this == other); }
};

}  // namespace dftracer::utils::components::text

// Hash specializations to enable caching
namespace std {
template <>
struct hash<dftracer::utils::components::text::Line> {
    std::size_t operator()(
        const dftracer::utils::components::text::Line& line) const noexcept {
        std::size_t h1 = std::hash<std::string>{}(line.content);
        std::size_t h2 = std::hash<std::size_t>{}(line.line_number);
        return h1 ^ (h2 << 1);
    }
};

template <>
struct hash<dftracer::utils::components::text::Lines> {
    std::size_t operator()(
        const dftracer::utils::components::text::Lines& lines) const noexcept {
        std::size_t h = 0;
        for (const auto& line : lines.lines) {
            h ^= std::hash<dftracer::utils::components::text::Line>{}(line) +
                 0x9e3779b9 + (h << 6) + (h >> 2);
        }
        return h;
    }
};

template <>
struct hash<dftracer::utils::components::text::Text> {
    std::size_t operator()(
        const dftracer::utils::components::text::Text& text) const noexcept {
        return std::hash<std::string>{}(text.content);
    }
};

template <>
struct hash<dftracer::utils::components::text::Hash> {
    std::size_t operator()(const dftracer::utils::components::text::Hash&
                               hash_val) const noexcept {
        return std::hash<std::size_t>{}(hash_val.value);
    }
};
}  // namespace std

#endif  // DFTRACER_UTILS_COMPONENTS_TEXT_SHARED_H
