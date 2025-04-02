#ifndef __GMSSL_EXPORT_H__
#define __GMSSL_EXPORT_H__

#ifdef __cplusplus
extern "C"
{
#endif

#if !defined(__WINDOWS__) && (defined(WIN32) || defined(WIN64) || defined(_MSC_VER) || defined(_WIN32))
#define __WINDOWS__
#endif

#ifdef __WINDOWS__
#define EXPORT_API __declspec(dllexport)
#else
#define EXPORT_API __attribute__((visibility("default")))
#endif


#ifdef __cplusplus
}
#endif

#endif // __GMSSL_EXPORT_H__
