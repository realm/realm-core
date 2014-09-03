#ifndef TIGHTDB_SHARED_PTR_HPP
#define TIGHTDB_SHARED_PTR_HPP

namespace tightdb {
namespace util {

template <class T> class SharedPtr 
{
public:
    SharedPtr(T* p) : m_ptr(p), m_count(new std::size_t(1)) {}

    SharedPtr() : m_ptr(0), m_count(nil()) 
    {
        incref(); 
    }

    ~SharedPtr() 
    { 
        decref(); 
    }
                        
    SharedPtr(const SharedPtr<T>& o) : m_ptr(o.m_ptr), m_count(o.m_count)
    { 
        incref(); 
    }
              
    SharedPtr<T>& operator=(const SharedPtr<T>& o) {
        if (m_ptr == o.m_ptr) 
            return *this;
        decref();
        m_ptr = o.m_ptr;
        m_count = o.m_count;
        incref();
        return *this;                          
    }
                        
    T* get() 
    {
        return m_ptr;
    }

    T* operator->() 
    { 
        return m_ptr;
    }

    T& operator*() 
    {
        return *m_ptr;
    }
                        
    const T* get() const 
    {
        return m_ptr;
    }

    const T* operator->() const 
    {
        return m_ptr;
    }

    const T& operator*() const
    { 
        return *m_ptr;
    }
                        
    bool operator==(const SharedPtr<T>& o) const
    { 
        return m_ptr == o.m_ptr;
    }

    bool operator!=(const SharedPtr<T>& o) const
    { 
        return m_ptr != o.m_ptr;
    }

    bool operator<(const SharedPtr<T>& o) const
    { 
        return m_ptr < o.m_ptr;
    }
                        
    std::size_t refm_count() const
    {
        return *m_count;
    }
                        
private:
    // special case, null pointer (nil-code)
    static std::size_t* nil()
    { 
        static std::size_t nil_m_counter(1);
        return &nil_m_counter; 
    }

    void decref() 
    { 
        if (--(*m_count) == 0) {
            delete m_ptr;
            delete m_count;
        } 
    }

    void incref() 
    {
        ++(*m_count); 
    }

    T* m_ptr;
    std::size_t* m_count;
};

}
}

#endif