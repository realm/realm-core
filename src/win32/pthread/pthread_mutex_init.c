/*
 * pthread_mutex_init.c
 *
 * Description:
 * This translation unit implements mutual exclusion (mutex) primitives.
 *
 * --------------------------------------------------------------------------
 *
 *      Pthreads-win32 - POSIX Threads Library for Win32
 *      Copyright(C) 1998 John E. Bossom
 *      Copyright(C) 1999,2005 Pthreads-win32 contributors
 * 
 *      Contact Email: rpj@callisto.canberra.edu.au
 * 
 *      The current list of contributors is contained
 *      in the file CONTRIBUTORS included with the source
 *      code distribution. The list can also be seen at the
 *      following World Wide Web location:
 *      http://sources.redhat.com/pthreads-win32/contributors.html
 * 
 *      This library is free software; you can redistribute it and/or
 *      modify it under the terms of the GNU Lesser General Public
 *      License as published by the Free Software Foundation; either
 *      version 2 of the License, or (at your option) any later version.
 * 
 *      This library is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *      Lesser General Public License for more details.
 * 
 *      You should have received a copy of the GNU Lesser General Public
 *      License along with this library in the file COPYING.LIB;
 *      if not, write to the Free Software Foundation, Inc.,
 *      59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 */

#include "pthread.h"
#include "implement.h"
#include <Objbase.h>
#include <assert.h>

int
pthread_mutex_init (pthread_mutex_t * mutex, const pthread_mutexattr_t * attr)
{
  int result = 0;
  pthread_mutex_t mx;

  if (mutex == NULL)
    {
      return EINVAL;
    }

  if (attr != NULL && *attr != NULL)
    {
      if ((*attr)->pshared == PTHREAD_PROCESS_SHARED)
        {
           GUID guid;
           HANDLE h;  

            /*
           * Creating mutex that can be shared between
           * processes.
           */

          // IF YOU PAGEFAULT HERE, IT'S LIKELY CAUSED BY DATABASE RESIDING ON NETWORK SHARE (WINDOWS + *NIX). Memory 
          // mapping is not coherent there. Note that this issue is NOT pthread related. Only reason why it happens in 
          // this mutex->is_shared is that mutex coincidentally happens to be the first member that shared group accesses.
          mutex->is_shared = 1; // <-- look above!
          // ^^^^ Look above

          // Create unique and random mutex name. UuidCreate() needs linking with Rpcrt4.lib, so we use CoCreateGuid() 
          // instead. That way end-user won't need to mess with Visual Studio project settings
          CoCreateGuid(&guid);
          sprintf_s(mutex->shared_name, sizeof(mutex->shared_name), "Global\\%08X%04X%04X%02X%02X%02X%02X%02X", guid.Data1, guid.Data2, guid.Data3, guid.Data4[0], guid.Data4[1], guid.Data4[2], guid.Data4[3], guid.Data4[4]);
          h = CreateMutexA(NULL, 0, mutex->shared_name);
          if(h == NULL)
              return EAGAIN;

          mutex->cached_handle = h;
          mutex->cached_pid = getpid();
          mutex->cached_windows_pid = GetCurrentProcessId();

          return 0;
        }
    }

  mx.original = (struct pthread_mutex_t_*) calloc (1, sizeof (*mx.original));
  mx.is_shared = 0;

  if (mx.original == NULL)
    {
      result = ENOMEM;
    }
  else
    {
      mx.original->lock_idx = 0;
      mx.original->recursive_count = 0;
      mx.original->robustNode = NULL;
      if (attr == NULL || *attr == NULL)
        {
          mx.original->kind = PTHREAD_MUTEX_DEFAULT;
        }
      else
        {
          mx.original->kind = (*attr)->kind;
          if ((*attr)->robustness == PTHREAD_MUTEX_ROBUST)
            {
              /*
               * Use the negative range to represent robust types.
               * Replaces a memory fetch with a register negate and incr
               * in pthread_mutex_lock etc.
               *
               * Map 0,1,..,n to -1,-2,..,(-n)-1
               */
              mx.original->kind = -mx.original->kind - 1;

              mx.original->robustNode = (ptw32_robust_node_t*) malloc(sizeof(ptw32_robust_node_t));
              mx.original->robustNode->stateInconsistent = PTW32_ROBUST_CONSISTENT;
              mx.original->robustNode->mx = mx;
              mx.original->robustNode->next = NULL;
              mx.original->robustNode->prev = NULL;
            }
        }

      mx.original->ownerThread.p = NULL;

      mx.original->event = CreateEvent (NULL, PTW32_FALSE,    /* manual reset = No */
                              PTW32_FALSE,           /* initial state = not signaled */
                              NULL);                 /* event name */

      if (0 == mx.original->event)
        {
          result = ENOSPC;
          free (mx.original);
          mx.original = NULL;
        }
    }

  memcpy(mutex, &mx, sizeof(mx));
  return (result);
}
