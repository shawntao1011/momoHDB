#include <iomanip>
#include <ios>
#include <k.h>
#include <string>
#include "print_k.hpp"

#include <climits>

std::string indent(int depth) {
    return std::string(static_cast<std::size_t>(depth) * 2, ' ');
}

void print_atom(K x, std::ostream& os) {
    switch (x->t) {
        case -KS:
            os << '`' << (x->s ? x->s : "");
            return;
        case -KC:
            os << "'" << static_cast<char>(x->g) << "'";
            return;
        case -KG:
            os << "0x" << std::hex << std::setw(2) << std::setfill('0') << (0xFF & x->g) << std::dec;
            return;
        case -KI:
            os << x->i;
            return;
        case -KJ:
            os << x->j;
            return;
        case -KF:
            os << x->f;
            return;
        case -KE:
            os << x->e;
            return;
        case -KP:
            os << "ts2000ns(" << x->j << ')';
            return;
        default:
            os << "<atom t=" << x->t << ">";
            return;
    }
}

void print_list(K x, std::ostream& os, int depth) {
    switch (x->t) {
        case KC: {
            os << '"' << std::string(reinterpret_cast<char*>(x->G0), static_cast<std::size_t>(x->n)) << '"';
            return;
        }
        case KS: {
            os << "`";
            for (J i = 0; i < x->n; ++i) {
                if (i > 0) os << '`';
                os << (kS(x)[i] ? kS(x)[i] : "");
            }
            return;
        }
        case KG: {
            os << "0x";
            const J shown = std::min<J>(x->n, 16);
            for (J i = 0; i < shown; ++i) {
                os << std::hex << std::setw(2) << std::setfill('0') << (0xFF & kG(x)[i]);
            }
            if (x->n > shown) os << "...";
            os << std::dec;
            return;
        }
        case 0: {
            os << "[";
            for (J i = 0; i < x->n; ++i) {
                if (i > 0) os << ", ";
                print_k(kK(x)[i], os, depth + 1);
            }
            os << "]";
            return;
        }
        default:
            os << "<list t=" << x->t << " n=" << x->n << ">";
            return;
    }
}

void print_dict(K x, std::ostream& os, int depth) {
    K keys = kK(x)[0];
    K vals = kK(x)[1];

    if (!keys || keys->t != KS || !vals || vals->t != 0) {
        os << "<dict>";
        return;
    }

    os << "{\n";
    for (J i = 0; i < keys->n; ++i) {
        os << indent(depth + 1) << (kS(keys)[i] ? kS(keys)[i] : "") << ": ";
        print_k(kK(vals)[i], os, depth + 1);
        os << "\n";
    }
    os << indent(depth) << "}";
}

void print_k(K x, std::ostream& os, int depth) {
    if (!x) {
        os << "<null>";
        return;
    }

    if (x->t < 0) {
        print_atom(x, os);
        return;
    }

    if (x->t == 99) {
        print_dict(x, os, depth);
        return;
    }

    if (x->t == 98) {
        os << "<table>";
        return;
    }

    print_list(x, os, depth);
}