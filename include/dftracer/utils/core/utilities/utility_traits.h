#ifndef DFTRACER_UTILS_CORE_UTILITIES_UTILITY_TRAITS_H
#define DFTRACER_UTILS_CORE_UTILITIES_UTILITY_TRAITS_H

#include <type_traits>

namespace dftracer::utils {
// Forward declaration
class TaskContext;
}  // namespace dftracer::utils

namespace dftracer::utils::utilities {

namespace detail {

/**
 * @brief SFINAE trait to detect if a type has process(I, TaskContext&) method.
 *
 * Primary template (false case) - used when the method doesn't exist.
 */
template <typename T, typename I, typename O, typename = void>
struct has_process_with_context_impl : std::false_type {};

/**
 * @brief Specialization for types that have process(I, TaskContext&) method.
 *
 * Uses std::void_t to detect if the expression is valid. If it compiles,
 * this specialization is chosen and inherits from std::true_type.
 */
template <typename T, typename I, typename O>
struct has_process_with_context_impl<
    T, I, O,
    std::void_t<decltype(std::declval<T&>().process(
        std::declval<const I&>(), std::declval<TaskContext&>()))>>
    : std::true_type {};

/**
 * @brief Convenience variable template for has_process_with_context.
 *
 * Usage:
 * @code
 * if constexpr (detail::has_process_with_context_v<MyUtility, Input, Output>) {
 *     // Utility has process(I, TaskContext&)
 * }
 * @endcode
 */
template <typename T, typename I, typename O>
inline constexpr bool has_process_with_context_v =
    has_process_with_context_impl<T, I, O>::value;

/**
 * @brief SFINAE trait to detect if a type has process(I) method.
 *
 * Primary template (false case) - used when the method doesn't exist.
 */
template <typename T, typename I, typename O, typename = void>
struct has_process_impl : std::false_type {};

/**
 * @brief Specialization for types that have process(I) method.
 *
 * Uses std::void_t to detect if the expression is valid.
 */
template <typename T, typename I, typename O>
struct has_process_impl<
    T, I, O,
    std::void_t<decltype(std::declval<T&>().process(std::declval<const I&>()))>>
    : std::true_type {};

/**
 * @brief Convenience variable template for has_process.
 *
 * Usage:
 * @code
 * if constexpr (detail::has_process_v<MyUtility, Input, Output>) {
 *     // Utility has process(I)
 * }
 * @endcode
 */
template <typename T, typename I, typename O>
inline constexpr bool has_process_v = has_process_impl<T, I, O>::value;

}  // namespace detail

}  // namespace dftracer::utils::utilities

#endif  // DFTRACER_UTILS_CORE_UTILITIES_UTILITY_TRAITS_H
