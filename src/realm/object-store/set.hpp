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

    const ObjectSchema& get_object_schema() const;

    bool is_valid() const;
    void verify_attached() const;
    void verify_in_transaction() const;

    size_t size() const;

    template <class T>
    size_t find(const T&) const;
    template <class T>
    std::pair<size_t, bool> insert(T);
    template <class T>
    std::pair<size_t, bool> remove(const T&);

    template <class T, class Context>
    size_t find(Context&, const T&) const;
    template <class T, class Context>
    std::pair<size_t, bool> insert(Context&, const T& value, CreatePolicy = CreatePolicy::SetLink);
    template <class T, class Context>
    std::pair<size_t, bool> remove(Context&, const T&);

    Results sort(SortDescriptor order) const;
    Results sort(const std::vector<std::pair<std::string, bool>>& keypaths) const;
    Results filter(Query q) const;

    Results as_results() const;
    Results snapshot() const;

    Set freeze(const std::shared_ptr<Realm>& realm) const;
    bool is_frozen() const noexcept;

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

template <class Fn>
auto Set::dispatch(Fn&& fn) const
{
    verify_attached();
    return switch_on_type(get_type(), std::forward<Fn>(fn));
}

template <class T>
auto& Set::as() const
{
    REALM_ASSERT(dynamic_cast<realm::Set<T>*>(m_set_base.get()));
    return static_cast<realm::Set<T>&>(*m_set_base);
}

template <class T, class Context>
size_t Set::find(Context& ctx, const T& value) const
{
    return dispatch([&](auto t) {
        return this->find(ctx.template unbox<std::decay_t<decltype(*t)>>(value, CreatePolicy::Skip));
    });
}

template <class T, class Context>
std::pair<size_t, bool> Set::insert(Context& ctx, const T& value, CreatePolicy policy)
{
    return dispatch([&](auto t) {
        return this->insert(ctx.template unbox<std::decay_t<decltype(*t)>>(value, policy));
    });
}

template <class T, class Context>
std::pair<size_t, bool> Set::remove(Context& ctx, const T& value)
{
    return dispatch([&](auto t) {
        return this->remove(ctx.template unbox<std::decay_t<decltype(*t)>>(value));
    });
}

} // namespace realm::object_store

#endif // REALM_OS_SET_HPP
