--------------------------------------------------------------------------------
-- CLIENT ----------------------------------------------------------------------
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
-- EventClientCreate -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientCreate (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
BEGIN
  PERFORM WriteToEventLog('M', 1010, 'Клиент создан.', pObject);

  PERFORM ExecuteObjectAction(pObject, GetAction('enable'));
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientOpen -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientOpen (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
BEGIN
  PERFORM WriteToEventLog('M', 1011, 'Клиент открыт на просмотр.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientEdit -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientEdit (
  pObject	numeric default context_object(),
  pForm		jsonb default context_form()
) RETURNS	void
AS $$
DECLARE
  old_email	jsonb;
  new_email	jsonb;
BEGIN
  old_email = pForm#>'{old, email}';
  new_email = pForm#>'{new, email}';

  IF coalesce(old_email, '{}') <> coalesce(new_email, '{}') THEN
    PERFORM EventConfirmEmail(pObject, new_email);
  END IF;

  PERFORM WriteToEventLog('M', 1012, 'Клиент изменён.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientSave -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientSave (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
BEGIN
  PERFORM WriteToEventLog('M', 1013, 'Клиент сохранён.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientEnable -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientEnable (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
DECLARE
  r             record;

  nId           numeric;
  nArea         numeric;
  nUserId       numeric;
  nInterface    numeric;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;

  IF nUserId IS NOT NULL THEN
    PERFORM UserUnLock(nUserId);

    PERFORM DeleteGroupForMember(nUserId, GetGroup('guest'));

    PERFORM AddMemberToGroup(nUserId, GetGroup('user'));

    nArea := GetArea('default');
    SELECT * INTO nId FROM db.member_area WHERE area = nArea AND member = nUserId;
    IF NOT FOUND THEN
      PERFORM AddMemberToArea(nUserId, nArea);
      PERFORM SetDefaultArea(nArea, nUserId);
    END IF;

    nInterface := GetInterface('I:1:0:0');
    SELECT * INTO nId FROM db.member_interface WHERE interface = nInterface AND member = nUserId;
    IF NOT FOUND THEN
      PERFORM AddMemberToInterface(nUserId, nInterface);
    END IF;

    nInterface := GetInterface('I:1:0:3');
    SELECT * INTO nId FROM db.member_interface WHERE interface = nInterface AND member = nUserId;
    IF NOT FOUND THEN
      PERFORM AddMemberToInterface(nUserId, nInterface);
      PERFORM SetDefaultInterface(nInterface, nUserId);
    END IF;

    FOR r IN SELECT code FROM db.session WHERE userid = nUserId
    LOOP
      PERFORM SetArea(GetDefaultArea(nUserId), nUserId, r.code);
      PERFORM SetInterface(GetDefaultInterface(nUserId), nUserId, r.code);
    END LOOP;

    PERFORM EventConfirmEmail(pObject);
  END IF;

  PERFORM WriteToEventLog('M', 1014, 'Клиент утверждён.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientDisable ----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientDisable (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
DECLARE
  r         record;
  nUserId	numeric;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;

  IF nUserId IS NOT NULL THEN
    PERFORM UserLock(nUserId);

    PERFORM DeleteGroupForMember(nUserId);
    PERFORM DeleteAreaForMember(nUserId);
    PERFORM DeleteInterfaceForMember(nUserId);

    PERFORM AddMemberToGroup(nUserId, GetGroup('guest'));
    PERFORM AddMemberToArea(nUserId, GetArea('guest'));

    PERFORM SetDefaultArea(GetArea('guest'), nUserId);
    PERFORM SetDefaultInterface(GetInterface('I:1:0:4'), nUserId);

    FOR r IN SELECT code FROM db.session WHERE userid = nUserId
    LOOP
      PERFORM SetArea(GetDefaultArea(nUserId), nUserId, r.code);
      PERFORM SetInterface(GetDefaultInterface(nUserId), nUserId, r.code);
    END LOOP;
  END IF;

  PERFORM WriteToEventLog('M', 1015, 'Клиент закрыт.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientDelete -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientDelete (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
DECLARE
  nUserId	numeric;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;

  IF nUserId IS NOT NULL THEN
  END IF;

  IF nUserId IS NOT NULL THEN
    DELETE FROM db.session WHERE userid = nUserId;

    PERFORM UserLock(nUserId);

    PERFORM DeleteGroupForMember(nUserId);
    PERFORM DeleteAreaForMember(nUserId);
    PERFORM DeleteInterfaceForMember(nUserId);

    UPDATE db.user SET pswhash = null WHERE id = nUserId;
  END IF;

  PERFORM WriteToEventLog('M', 1016, 'Клиент удалён.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientRestore ----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientRestore (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
BEGIN
  PERFORM WriteToEventLog('M', 1017, 'Клиент восстановлен.', pObject);
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientDrop -------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientDrop (
  pObject	numeric default context_object()
) RETURNS	void
AS $$
DECLARE
  r		    record;
  nUserId   numeric;
BEGIN
  SELECT label INTO r FROM db.object WHERE id = pObject;

  SELECT userid INTO nUserId FROM client WHERE id = pObject;
  IF nUserId IS NOT NULL THEN
    UPDATE db.client SET userid = null WHERE id = pObject;
    DELETE FROM db.session WHERE userid = nUserId;
    PERFORM DeleteUser(nUserId);
  END IF;

  DELETE FROM db.client_name WHERE client = pObject;
  DELETE FROM db.client WHERE id = pObject;

  PERFORM WriteToEventLog('W', 2010, '[' || pObject || '] [' || coalesce(r.label, '<null>') || '] Клиент уничтожен.');
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientConfirm ----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientConfirm (
  pObject	    numeric default context_object()
) RETURNS	    void
AS $$
DECLARE
  nUserId       numeric;
  vEmail        text;
  bVerified     bool;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;

  IF nUserId IS NOT NULL THEN

	SELECT email, email_verified INTO vEmail, bVerified
	  FROM db.user u INNER JOIN db.profile p ON u.id = p.userid AND u.type = 'U'
	 WHERE id = nUserId;

	IF vEmail IS NULL THEN
      PERFORM EmailAddressNotSet();
    END IF;

    IF NOT bVerified THEN
      PERFORM EmailAddressNotVerified(vEmail);
    END IF;

    PERFORM EventAccountInfo(pObject);
  END IF;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventClientReconfirm --------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventClientReconfirm (
  pObject	    numeric default context_object()
) RETURNS	    void
AS $$
DECLARE
  nUserId       numeric;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;
  IF nUserId IS NOT NULL THEN
	UPDATE db.profile SET email_verified = false WHERE userid = nUserId;
    PERFORM EventConfirmEmail(pObject);
  END IF;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventConfirmEmail -----------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventConfirmEmail (
  pObject		numeric default context_object(),
  pForm		    jsonb default context_form()
) RETURNS		void
AS $$
DECLARE
  nUserId       numeric;
  vCode			text;
  vName			text;
  vDomain       text;
  vUserName     text;
  vEmail		text;
  vProject		text;
  vHost         text;
  vNoReply      text;
  vSupport		text;
  vSubject      text;
  vText			text;
  vHTML			text;
  vBody			text;
  vDescription  text;
  bVerified		bool;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;
  IF nUserId IS NOT NULL THEN

    IF pForm IS NOT NULL THEN
	  UPDATE db.client SET email = pForm WHERE id = nUserId;
	END IF;

	SELECT username, name, email, email_verified, locale INTO vUserName, vName, vEmail, bVerified
	  FROM db.user u INNER JOIN db.profile p ON u.id = p.userid AND u.type = 'U'
	 WHERE id = nUserId;

	IF vEmail IS NOT NULL AND NOT bVerified THEN

	  vProject := (RegGetValue(RegOpenKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Name')).vString;
	  vHost := (RegGetValue(RegOpenKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Host')).vString;
	  vDomain := (RegGetValue(RegOpenKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Domain')).vString;

	  vCode := GetVerificationCode(NewVerificationCode(nUserId));

	  vNoReply := format('noreply@%s', vDomain);
	  vSupport := format('support@%s', vDomain);

	  IF locale_code() = 'ru' THEN
        vSubject := 'Подтвердите, пожалуйста, адрес Вашей электронной почты.';
        vDescription := 'Подтверждение email: ' || vEmail;
	  ELSE
        vSubject := 'Please confirm your email address.';
        vDescription := 'Confirm email: ' || vEmail;
	  END IF;

	  vText := GetConfirmEmailText(vName, vUserName, vCode, vProject, vHost, vSupport);
	  vHTML := GetConfirmEmailHTML(vName, vUserName, vCode, vProject, vHost, vSupport);

	  vBody := CreateMailBody(vProject, vNoReply, null, vEmail, vSubject, vText, vHTML);

      PERFORM SendMessage(CreateMessage(pObject, GetType('message.outbox'), GetAgent('smtp.agent'), vNoReply, vEmail, vSubject, vBody, vDescription));
      PERFORM WriteToEventLog('M', 1110, vDescription, pObject);
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- EventAccountInfo ------------------------------------------------------------
--------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION EventAccountInfo (
  pObject		numeric default context_object()
) RETURNS		void
AS $$
DECLARE
  nUserId       numeric;
  vSecret       text;
  vName			text;
  vDomain       text;
  vUserName     text;
  vEmail		text;
  vProject		text;
  vHost         text;
  vNoReply      text;
  vSupport		text;
  vSubject      text;
  vText			text;
  vHTML			text;
  vBody			text;
  vDescription  text;
  bVerified		bool;
BEGIN
  SELECT userid INTO nUserId FROM db.client WHERE id = pObject;
  IF nUserId IS NOT NULL THEN

	SELECT username, name, encode(hmac(secret::text, GetSecretKey(), 'sha512'), 'hex'), email, email_verified INTO vUserName, vName, vSecret, vEmail, bVerified
	  FROM db.user u INNER JOIN db.profile p ON u.id = p.userid AND u.type = 'U'
	 WHERE id = nUserId;

	IF vEmail IS NOT NULL AND bVerified THEN
	  vProject := (RegGetValue(RegOpenKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Name')).vString;
	  vHost := (RegGetValue(RegOpenKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Host')).vString;
	  vDomain := (RegGetValue(RegOpenKey('CURRENT_CONFIG', 'CONFIG\CurrentProject'), 'Domain')).vString;

	  vNoReply := format('noreply@%s', vDomain);
	  vSupport := format('support@%s', vDomain);

	  IF locale_code() = 'ru' THEN
        vSubject := 'Информация о Вашей учетной записи.';
        vDescription := 'Информация о учетной записи: ' || vUserName;
	  ELSE
        vSubject := 'Your account information.';
        vDescription := 'Account information: ' || vUserName;
	  END IF;

	  vText := GetAccountInfoText(vName, vUserName, vSecret, vProject, vSupport);
	  vHTML := GetAccountInfoHTML(vName, vUserName, vSecret, vProject, vSupport);

	  vBody := CreateMailBody(vProject, vNoReply, null, vEmail, vSubject, vText, vHTML);

      PERFORM SendMessage(CreateMessage(pObject, GetType('message.outbox'), GetAgent('smtp.agent'), vNoReply, vEmail, vSubject, vBody, vDescription));
      PERFORM WriteToEventLog('M', 1110, vDescription, pObject);
    END IF;
  END IF;
END;
$$ LANGUAGE plpgsql;
