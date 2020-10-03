/*++

Library name:

  apostol-core

Module Name:

  Processes.hpp

Notices:

  Application others processes

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

#ifndef APOSTOL_PROCESSES_HPP
#define APOSTOL_PROCESSES_HPP
//----------------------------------------------------------------------------------------------------------------------

#include "MessageServer/MessageServer.hpp"
#include "StreamServer/StreamServer.hpp"
//----------------------------------------------------------------------------------------------------------------------

static inline void CreateProcesses(CCustomProcess *AParent, CApplication *AApplication) {
    CMessageServer::CreateProcess(AParent, AApplication);
    CStreamServer::CreateProcess(AParent, AApplication);
}

#endif //APOSTOL_PROCESSES_HPP
