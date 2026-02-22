#include "kafkax/core/decoder.h"

extern "C" {

int kafkax_decoder_abi_version(void) {
  return KAFKAX_DECODER_ABI_VERSION;
}

int momo_decode_basicqot(const kafkax_envelope_t* env, kafkax_decode_out_t* out);
int momo_decode_orderbook(const kafkax_envelope_t* env, kafkax_decode_out_t* out);
int momo_decode_ticker(const kafkax_envelope_t* env, kafkax_decode_out_t* out);
int momo_decode_kl1min(const kafkax_envelope_t* env, kafkax_decode_out_t* out);

}
