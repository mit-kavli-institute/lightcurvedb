#pragma once
#include <exception>

template <typename T>
class Node {
    public:
        Node(T value) {
            this->value = value;
            this->left = nullptr;
            this->right = nullptr;
            this->parent = nullptr;
            this->red = true;
        };
        ~Node() {
            if (this->left) {
                delete this->left;
            }
            if (this->right) {
                delete this->right;
            }
        };

        Type value;
        Node *left;
        Node *right;
        Node *parent;
        bool red;
};

// Forward declaration to allow circular dependency
template <typename T, typename C = std::less< T > >
class RedblackTree;

template <typename T, typename C = std::less< T > >
class TreeIterator {
    public:
        TreeIterator() {
            this->current = nullptr;
            this->dir = 0;
        };
        TreeIterator(Node<T> *n, int d) {
            this->current = n;
            this->dir = d;
        };
        ~TreeIterator() {};


        TreeIterator<T,C>& operator=(const TreeIterator<T, C>&i) {
              this->current = i->current;
              this->dir = i->dir;
              return *this;
        };
        TreeIterator<T,C>& operator++() {
            if (!this->current) {
                // Nothing to increment on
                throw exception();
            }
            // Traverse the tree in 
            if (this->current->right) {

            }
        };
        T& operator*() const;
        bool operator==(const TreeIterator<T,C>&);
        bool operator!=(const TreeIterator<T,C>&);

        bool valid() const {
            return this->current != 0;
        };

        int getDir() const {
            return this->dir;
        };

    friend:
        class RedblackTree<T, C>;

    private:
        Node<T>* current;
        int dir;


    protected:
        Node<T>* getNode() const {
            return this->current;
        };
};

template <typename T, typename C>
class RedblackTree {
    private:
        void rotate_left(Node<T> *target);
        void rotate_right(Node<T> *target);
        
        Node<T> *root;
        C comp;

    public:
        RedblackTree();
        virtual ~RedblackTree();

        TreeIterator<T, C> find(T &value) const;
        bool insert(T, value, TreeIterator<T, C> &out_holder);
        bool remove(T, value, T &out_holder);

        TreeIterator<T, C> begin();
        TreeIterator<T, C> end();

    protected:
        Node<T> *getNode(TreeIterator<T,C> &it) const {
            return it.getNode();
        };
        bool remove(TreeIterator<T,C> &it, T &out_holder);
};
