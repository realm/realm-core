#ifndef REALM_OS_SET_HPP
#define REALM_OS_SET_HPP

#include <realm/object-store/object.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>

namespace realm::object_store {

class Set {
public:
    Set() noexcept;
    Set(std::shared_ptr<Realm> r, const Obj& parent_obj, ColKey col);
    Set(std::shared_ptr<Realm> r, const realm::SetBase& set);
    ~Set();

    Set(const Set&);
    Set& operator=(const Set&);
    Set(Set&&);
    Set& operator=(Set&&);

    const std::shared_ptr<Realm>& get_realm() const
    {
        return m_realm;
    }

    ColKey get_parent_column_key() const noexcept;
    ObjKey get_parent_object_key() const noexcept;
    TableKey get_parent_table_key() const noexcept;

    PropertyType get_type() const noexcept
    {
        return m_type;
    }

    const ObjectSchema& get_object_schema() const noexcept;

    bool is_valid() const noexcept;
    void verify_attached() const;
    void verify_in_transaction() const;

    size_t size() const;

    template <class T>
    size_t find(T const&) const;
    template <class T>
    std::pair<size_t, bool> insert(T);
    template <class T>
    bool remove(T const&);

    template <typename T, typename Context>
    std::pair<size_t, bool> insert(Context&, T&& value, CreatePolicy = CreatePolicy::SetLink);
    template <typename T, typename Context>
    void remove(Context&, T&& value);

    Results sort(SortDescriptor order) const;
    Results sort(const std::vector<std::pair<std::string, bool>>& keypaths) const;
    Results filter(Query q) const;

    Results as_results() const;
    Results snapshot() const;

    Set freeze(const std::shared_ptr<Realm>& realm) const;
    bool is_frozen() const noexcept;

    template <typename T, typename Context>
    size_t find(Context&, T&& value) const;

    util::Optional<Mixed> max(ColKey column = {}) const;
    util::Optional<Mixed> min(ColKey column = {}) const;
    util::Optional<Mixed> average(ColKey column = {}) const;

    bool operator==(const Set& rhs) const noexcept;

    NotificationToken add_notification_callback(CollectionChangeCallback cb) &;

    template <class T, class Context>
    void assign(Context&, T value, CreatePolicy = CreatePolicy::SetLink);

    struct InvalidatedException : public std::logic_error {
        InvalidatedException()
            : std::logic_error("Access to invalidated Set object")
        {
        }
    };

    struct InvalidEmbeddedOperationException : std::logic_error {
        InvalidEmbeddedOperationException()
            : std::logic_error("Cannot add an embedded object to a Set.")
        {
        }
    };

private:
    std::shared_ptr<Realm> m_realm;
    PropertyType m_type;
    mutable util::CopyableAtomic<const ObjectSchema*> m_object_schema = nullptr;
    // _impl::CollectionNotifier::Handle<_impl::ListNotifier> m_notifier;
    std::shared_ptr<realm::SetBase> m_set_base;

    template <class Fn>
    auto dispatch(Fn&&) const;
    template <class T>
    auto& as() const;

    friend struct std::hash<Set>;
};

template <typename Fn>
auto Set::dispatch(Fn&& fn) const
{
    verify_attached();
    return switch_on_type(get_type(), std::forward<Fn>(fn));
}

template <typename T, typename Context>
size_t Set::find(Context& ctx, T&& value) const
{
    return 1;
//    return dispatch([&](auto t) {
//        return this->find(ctx.template unbox<std::decay_t<decltype(*t)>>(value, CreatePolicy::Skip));
//    });
}

template <typename T, typename Context>
std::pair<size_t, bool> Set::insert(Context& ctx, T&& value, CreatePolicy policy)
{
    dispatch([&](auto t) {
        this->insert(ctx.template unbox<std::decay_t<decltype(*t)>>(value, policy));
    });
}

template <typename T, typename Context>
void Set::remove(Context& ctx, T&& value)
{

}

template <typename T>
auto& Set::as() const
{
    return static_cast<realm::Set<T>&>(*m_set_base);
}

//template <>
//inline auto& Set::as<Obj>() const
//{
//    return static_cast<realm::Set&>(*m_set_base);
//}


} // namespace realm::object_store

#endif // REALM_OS_SET_HPP
