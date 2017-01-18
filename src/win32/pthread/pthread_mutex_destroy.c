/*
 * pthread_mutex_destroy.c
 *
 * Description:
 * This translation unit implements mutual exclusion (mutex->original) primitives.
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

#if REALM_UWP
int _getpid(void);
#endif

int
pthread_mutex_destroy (pthread_mutex_t * mutex)
{
  int result = 0;
  pthread_mutex_t mx;

  if(mutex->is_shared)
  {
    BOOL d;
    int pid = _getpid();

    if(mutex->cached_pid != pid)
    {
        // Mutex destroyed by other process than process who called init. So duplicate handle and destroy mutex through that
        BOOL d2;
        HANDLE owner;

        // Get handle to process that called init
        owner = OpenProcess(PROCESS_ALL_ACCESS, 0, mutex->cached_windows_pid); 
        d2 = DuplicateHandle(owner, mutex->cached_handle, GetCurrentProcess(), &mutex->cached_handle, 0, FALSE, DUPLICATE_CLOSE_SOURCE); // Get handle to mutex, close owner's handle
        CloseHandle(owner); 

        if(d2 == 0)
            return 1;
    }

    // Close handle to mutex. Mutex should now be automatically destroyed by Windows because it has no open handles
    d = CloseHandle(mutex->cached_handle); 

    if(d == 0)
      return 1;

    return 0;
  }

  /*
   * Let the system deal with invalid pointers.
   */

  /*
   * Check to see if we have something to delete.
   */
  if ((void*)mutex->original < PTHREAD_ERRORCHECK_MUTEX)
    {
      mx.original = mutex->original;
      mx.is_shared = mutex->is_shared;

      result = pthread_mutex_trylock (&mx);

      /*
       * If trylock succeeded and the mutex->original is not recursively locked it
       * can be destroyed.
       */
      if (0 == result || ENOTRECOVERABLE == result)
	{
	  if (mx.original->kind != PTHREAD_MUTEX_RECURSIVE || 1 == mx.original->recursive_count)
	    {
	      /*
	       * FIXME!!!
	       * The mutex->original isn't held by another thread but we could still
	       * be too late invalidating the mutex->original below since another thread
	       * may already have entered mutex_lock and the check for a valid
	       * *mutex->original != NULL.
	       */
	      mutex->original = NULL;

	      result = (0 == result)?pthread_mutex_unlock(&mx):0;

	      if (0 == result)
		{
                  if (mx.original->robustNode != NULL)
                    {
                      free(mx.original->robustNode);
                    }
		  if (!CloseHandle (mx.original->event))
		    {
		      mutex->original = mx.original;
		      result = EINVAL;
		    }
		  else
		    {
		      free (mx.original);
		    }
		}
	      else
		{
		  /*
		   * Restore the mutex->original before we return the error.
		   */
		  mutex->original = mx.original;
		}
	    }
	  else			/* mx.original->recursive_count > 1 */
	    {
	      /*
	       * The mutex->original must be recursive and already locked by us (this thread).
	       */
	      mx.original->recursive_count--;	/* Undo effect of pthread_mutex_trylock() above */
	      result = EBUSY;
	    }
	}
    }
  else
    {
      ptw32_mcs_local_node_t node;

      /*
       * See notes in ptw32_mutex_check_need_init() above also.
       */

      ptw32_mcs_lock_acquire(&ptw32_mutex_test_init_lock, &node);

      /*
       * Check again.
       */
      if ((void*)mutex->original >= PTHREAD_ERRORCHECK_MUTEX)
	{
	  /*
	   * This is all we need to do to destroy a statically
	   * initialised mutex->original that has not yet been used (initialised).
	   * If we get to here, another thread
	   * waiting to initialise this mutex->original will get an EINVAL.
	   */
	  mutex->original = NULL;
	}
      else
	{
	  /*
	   * The mutex->original has been initialised while we were waiting
	   * so assume it's in use.
	   */
	  result = EBUSY;
	}
      ptw32_mcs_lock_release(&node);
    }

  return (result);
}
